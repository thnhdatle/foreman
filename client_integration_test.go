// SPDX-License-Identifier: Apache-2.0

//go:build integration

package foreman

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/thnhdatle/foreman/internal/testutil"
)

type countArgs struct {
	N int `json:"n"`
}

func (countArgs) Kind() string { return "count" }

type flakyArgs struct {
	Mode string `json:"mode"`
}

func (flakyArgs) Kind() string { return "flaky" }

func waitState(t *testing.T, c *Client, id int64, st State) *JobRow {
	t.Helper()
	var row *JobRow
	testutil.Eventually(t, func() bool {
		r, err := c.JobGet(context.Background(), id)
		require.NoError(t, err)
		row = r
		return r.State == st
	}, 20*time.Second, "job", id, "state", st)
	return row
}

func TestClient_RunsJobsAndRecordsOutcomes(t *testing.T) {
	t.Parallel()
	e := newIT(t)
	ctx := context.Background()
	var runs sync.Map
	workers := NewWorkers()
	AddWorker(workers, WorkFunc[flakyArgs](func(ctx context.Context, j *Job[flakyArgs]) error {
		n, _ := runs.LoadOrStore(j.ID, new(atomic.Int32))
		count := n.(*atomic.Int32).Add(1)
		switch j.Args.Mode {
		case "ok":
			return nil
		case "fail-once":
			if count == 1 {
				return errors.New("transient")
			}
			return nil
		case "snooze-once":
			if count == 1 {
				return Snooze(100 * time.Millisecond)
			}
			return nil
		case "discard":
			return Discard(errors.New("bad input"))
		case "cancel":
			return Cancel(errors.New("not needed"))
		case "panic":
			panic("boom")
		default:
			return errors.New("always")
		}
	}), WithRetryPolicy(ConstantBackoff{Delay: 50 * time.Millisecond}))
	c := e.started(t, Config{Queues: map[string]QueueConfig{"default": fastQueue(5)}, Workers: workers})

	insert := func(mode string, maxAttempts int) int64 {
		res, err := c.Insert(ctx, flakyArgs{Mode: mode}, &InsertOpts{MaxAttempts: maxAttempts})
		require.NoError(t, err)
		return res.Job.ID
	}
	ok := insert("ok", 3)
	retried := insert("fail-once", 3)
	snoozed := insert("snooze-once", 1)
	discarded := insert("discard", 3)
	cancelled := insert("cancel", 3)
	panicked := insert("panic", 2)
	exhausted := insert("always", 2)

	waitState(t, c, ok, StateCompleted)
	r := waitState(t, c, retried, StateCompleted)
	assert.Equal(t, 2, r.Attempt)
	require.Len(t, r.Errors, 1)
	assert.Equal(t, "transient", r.Errors[0].Error)
	r = waitState(t, c, snoozed, StateCompleted)
	assert.Equal(t, 1, r.Attempt, "snooze does not consume an attempt (max attempts 1)")
	r = waitState(t, c, discarded, StateDiscarded)
	assert.Contains(t, r.Errors[0].Error, "bad input")
	waitState(t, c, cancelled, StateCancelled)
	r = waitState(t, c, panicked, StateDiscarded)
	require.Len(t, r.Errors, 2)
	assert.Contains(t, r.Errors[0].Error, "panic: boom")
	assert.NotEmpty(t, r.Errors[0].Trace)
	r = waitState(t, c, exhausted, StateDiscarded)
	assert.Equal(t, 2, r.Attempt)

	// Dead letter: retry moves it back with attempt reset.
	rr, err := c.JobRetry(ctx, exhausted)
	require.NoError(t, err)
	assert.Equal(t, StateAvailable, rr.State)
	assert.Equal(t, 0, rr.Attempt)
	var meta map[string]any
	require.NoError(t, json.Unmarshal(rr.Metadata, &meta))
	assert.Equal(t, "discarded", meta["foreman_retried_from"])
}

func TestClient_InsertTxOnlyOnCommit(t *testing.T) {
	t.Parallel()
	e := newIT(t)
	ctx := context.Background()
	var ran atomic.Int32
	workers := NewWorkers()
	AddWorker(workers, WorkFunc[countArgs](func(context.Context, *Job[countArgs]) error { ran.Add(1); return nil }))
	c := e.started(t, Config{Queues: map[string]QueueConfig{"default": {MaxWorkers: 2, FetchInterval: time.Hour}}, Workers: workers})

	tx, err := e.pool.Begin(ctx)
	require.NoError(t, err)
	_, err = c.InsertTx(ctx, tx, countArgs{N: 1}, nil)
	require.NoError(t, err)
	_, err = c.InsertManyTx(ctx, tx, []InsertParams{{Args: countArgs{N: 2}}, {Args: countArgs{N: 3}}})
	require.NoError(t, err)
	require.NoError(t, tx.Rollback(ctx))

	tx, err = e.pool.Begin(ctx)
	require.NoError(t, err)
	_, err = c.InsertTx(ctx, tx, countArgs{N: 4}, nil)
	require.NoError(t, err)
	require.NoError(t, tx.Commit(ctx))
	// NOTIFY on commit wakes the producer although FetchInterval is 1h.
	testutil.Eventually(t, func() bool { return ran.Load() == 1 }, 10*time.Second)
	list, err := c.JobList(ctx, nil)
	require.NoError(t, err)
	assert.Len(t, list.Jobs, 1)
	_, err = c.InsertTx(ctx, nil, countArgs{}, nil)
	require.ErrorIs(t, err, ErrInvalidArgument)
}

func TestClient_InsertValidationAndUnique(t *testing.T) {
	t.Parallel()
	e := newIT(t)
	ctx := context.Background()
	c := e.client(t, Config{MaxArgsBytes: 64})
	_, err := c.Insert(ctx, flakyArgs{Mode: string(make([]byte, 100))}, nil)
	require.ErrorIs(t, err, ErrArgsTooLarge)
	_, err = c.Insert(ctx, countArgs{}, &InsertOpts{Priority: 9})
	require.ErrorIs(t, err, ErrInvalidInsertOpts)
	_, err = c.Insert(ctx, countArgs{}, &InsertOpts{Queue: "bad queue"})
	require.ErrorIs(t, err, ErrInvalidInsertOpts)
	_, err = c.Insert(ctx, countArgs{}, &InsertOpts{Metadata: json.RawMessage(`[1]`)})
	require.ErrorIs(t, err, ErrInvalidInsertOpts)
	_, err = c.Insert(ctx, countArgs{}, &InsertOpts{UniqueOpts: &UniqueOpts{ByArgs: true, ByArgFields: []string{"n"}}})
	require.ErrorIs(t, err, ErrInvalidInsertOpts)
	_, err = c.Insert(ctx, nil, nil)
	require.ErrorIs(t, err, ErrInvalidArgument)
	_, err = c.InsertMany(ctx, []InsertParams{{Args: countArgs{}}, {Args: countArgs{}, InsertOpts: &InsertOpts{MaxAttempts: -1}}})
	require.ErrorContains(t, err, "params[1]")

	u := &InsertOpts{UniqueOpts: &UniqueOpts{ByArgs: true}}
	a, err := c.Insert(ctx, countArgs{N: 1}, u)
	require.NoError(t, err)
	b, err := c.Insert(ctx, countArgs{N: 1}, u)
	require.NoError(t, err)
	assert.True(t, b.UniqueSkippedAsDuplicate)
	assert.Equal(t, a.Job.ID, b.Job.ID)
	other, err := c.Insert(ctx, countArgs{N: 2}, u)
	require.NoError(t, err)
	assert.False(t, other.UniqueSkippedAsDuplicate)
	byKind := &InsertOpts{UniqueOpts: &UniqueOpts{}}
	k1, err := c.Insert(ctx, flakyArgs{Mode: "x"}, byKind)
	require.NoError(t, err)
	k2, err := c.Insert(ctx, flakyArgs{Mode: "y"}, byKind)
	require.NoError(t, err)
	assert.Equal(t, k1.Job.ID, k2.Job.ID, "unique by kind only")
}

type defaultsArgs struct{}

func (defaultsArgs) Kind() string { return "defaults" }
func (defaultsArgs) InsertOpts() InsertOpts {
	return InsertOpts{Queue: "kindq", Priority: 3, Tags: []string{"a"}, MaxAttempts: 7}
}

func TestClient_InsertOptsPrecedence(t *testing.T) {
	t.Parallel()
	e := newIT(t)
	ctx := context.Background()
	workers := NewWorkers()
	AddWorker(workers, WorkFunc[countArgs](func(context.Context, *Job[countArgs]) error { return nil }), WithMaxAttempts(4))
	c := e.client(t, Config{Workers: workers})
	r, err := c.Insert(ctx, defaultsArgs{}, &InsertOpts{Priority: 1})
	require.NoError(t, err)
	assert.Equal(t, "kindq", r.Job.Queue)
	assert.Equal(t, 1, r.Job.Priority, "explicit wins")
	assert.Equal(t, []string{"a"}, r.Job.Tags)
	assert.Equal(t, 7, r.Job.MaxAttempts)
	r, err = c.Insert(ctx, countArgs{}, nil)
	require.NoError(t, err)
	assert.Equal(t, 4, r.Job.MaxAttempts, "worker option")
	assert.Equal(t, DefaultQueue, r.Job.Queue)
	assert.Equal(t, DefaultPriority, r.Job.Priority)
	future := time.Now().Add(time.Hour)
	r, err = c.Insert(ctx, countArgs{}, &InsertOpts{ScheduledAt: future, Timeout: time.Second, ConcurrencyKey: "t1"})
	require.NoError(t, err)
	assert.Equal(t, StateScheduled, r.Job.State)
	assert.Equal(t, time.Second, r.Job.Timeout)
	assert.Equal(t, "t1", r.Job.ConcurrencyKey)
	args, err := DecodeArgs[countArgs](r.Job)
	require.NoError(t, err)
	assert.Equal(t, countArgs{}, args)
	_, err = DecodeArgs[flakyArgs](r.Job)
	require.ErrorIs(t, err, ErrInvalidArgument)
}

func TestClient_TenThousandJobsThreeClientsRunOnce(t *testing.T) {
	t.Parallel()
	e := newIT(t)
	ctx := context.Background()
	const total = 10_000
	var mu sync.Mutex
	runs := make(map[int]int, total)
	workers := NewWorkers()
	AddWorker(workers, WorkFunc[countArgs](func(_ context.Context, j *Job[countArgs]) error {
		mu.Lock()
		runs[j.Args.N]++
		mu.Unlock()
		return nil
	}))
	cfg := Config{Queues: map[string]QueueConfig{"default": fastQueue(20)}, Workers: workers}
	clients := []*Client{e.started(t, cfg), e.started(t, cfg), e.started(t, cfg)}
	params := make([]InsertParams, total)
	for i := range params {
		params[i] = InsertParams{Args: countArgs{N: i}}
	}
	for start := 0; start < total; start += 1000 {
		_, err := clients[start/1000%3].InsertMany(ctx, params[start:start+1000])
		require.NoError(t, err)
	}
	testutil.Eventually(t, func() bool {
		var n int
		require.NoError(t, e.pool.QueryRow(ctx, `SELECT count(*) FROM "`+e.schema+`".foreman_jobs WHERE state = 'completed'`).Scan(&n))
		return n == total
	}, 2*time.Minute)
	mu.Lock()
	defer mu.Unlock()
	assert.Len(t, runs, total)
	for n, count := range runs {
		if count != 1 {
			t.Fatalf("job %d ran %d times", n, count)
		}
	}
	byClient := map[string]int{}
	rows, err := e.pool.Query(ctx, `SELECT attempted_by[1], count(*) FROM "`+e.schema+`".foreman_jobs GROUP BY 1`)
	require.NoError(t, err)
	for rows.Next() {
		var id string
		var n int
		require.NoError(t, rows.Scan(&id, &n))
		byClient[id] = n
	}
	require.NoError(t, rows.Err())
	assert.Len(t, byClient, 3, "all three clients worked: %v", byClient)
}

func TestClient_CancelInEachState(t *testing.T) {
	t.Parallel()
	e := newIT(t)
	ctx := context.Background()
	started := make(chan int64, 1)
	var cause atomic.Value
	workers := NewWorkers()
	AddWorker(workers, WorkFunc[countArgs](func(ctx context.Context, j *Job[countArgs]) error {
		started <- j.ID
		<-ctx.Done()
		cause.Store(fmt.Sprint(context.Cause(ctx)))
		return ctx.Err()
	}))
	owner := e.started(t, Config{Queues: map[string]QueueConfig{"default": fastQueue(1)}, Workers: workers})
	admin := e.client(t, Config{}) // producer-only client on the same database

	running, err := owner.Insert(ctx, countArgs{N: 1}, nil)
	require.NoError(t, err)
	id := <-started
	require.Equal(t, running.Job.ID, id)
	r, err := admin.JobCancel(ctx, id)
	require.NoError(t, err)
	assert.Equal(t, StateRunning, r.State)
	assert.NotNil(t, r.CancelRequestedAt)
	waitState(t, admin, id, StateCancelled)
	assert.Contains(t, cause.Load(), "cancel requested")
	r, err = admin.JobCancel(ctx, id)
	require.NoError(t, err, "cancelling a cancelled job is idempotent")
	assert.Equal(t, StateCancelled, r.State)

	future := time.Now().Add(time.Hour)
	for _, opts := range []*InsertOpts{{Queue: "unserved"}, {ScheduledAt: future}} {
		res, err := admin.Insert(ctx, countArgs{N: 2}, opts)
		require.NoError(t, err)
		r, err := admin.JobCancel(ctx, res.Job.ID)
		require.NoError(t, err)
		assert.Equal(t, StateCancelled, r.State)
		require.NotNil(t, r.FinalizedAt)
	}
	_, err = admin.JobCancel(ctx, 1<<40)
	require.ErrorIs(t, err, ErrNotFound)

	done, err := admin.Insert(ctx, countArgs{}, &InsertOpts{Queue: "unserved"})
	require.NoError(t, err)
	_, err = admin.JobDiscard(ctx, done.Job.ID)
	require.NoError(t, err)
	_, err = admin.JobCancel(ctx, done.Job.ID)
	require.ErrorIs(t, err, ErrInvalidTransition)
	_, err = admin.JobDiscard(ctx, done.Job.ID)
	require.NoError(t, err, "discarding a discarded job is idempotent")
}

func TestClient_PauseResumeAcrossTwoClients(t *testing.T) {
	t.Parallel()
	e := newIT(t)
	ctx := context.Background()
	var ran atomic.Int32
	workers := NewWorkers()
	AddWorker(workers, WorkFunc[countArgs](func(context.Context, *Job[countArgs]) error { ran.Add(1); return nil }))
	cfg := Config{Queues: map[string]QueueConfig{"default": {MaxWorkers: 2, FetchInterval: 200 * time.Millisecond}}, Workers: workers}
	a := e.started(t, cfg)
	b := e.started(t, cfg)
	require.NoError(t, a.QueuePause(ctx, "default"))
	testutil.Eventually(t, func() bool { return b.run != nil && b.run.producers["default"].Paused() }, 5*time.Second,
		"pause reaches the other client")
	for i := 0; i < 20; i++ {
		_, err := b.Insert(ctx, countArgs{N: i}, nil)
		require.NoError(t, err)
	}
	deadline := time.Now().Add(time.Second) // several fetch intervals while paused
	testutil.Eventually(t, func() bool { return time.Now().After(deadline) }, 2*time.Second)
	assert.Zero(t, ran.Load())
	qs, err := a.QueueList(ctx)
	require.NoError(t, err)
	require.NotEmpty(t, qs)
	assert.True(t, qs[0].Paused)
	assert.EqualValues(t, 20, qs[0].Available)
	require.NoError(t, b.QueueResume(ctx, "default"))
	testutil.Eventually(t, func() bool { return ran.Load() == 20 }, 10*time.Second)
	require.Error(t, a.QueuePause(ctx, "bad name"))
}

func TestClient_CompleteTxAndJobWork(t *testing.T) {
	t.Parallel()
	e := newIT(t)
	ctx := context.Background()
	workers := NewWorkers()
	AddWorker(workers, WorkFunc[countArgs](func(ctx context.Context, j *Job[countArgs]) error {
		tx, err := ClientFromContext(ctx).Pool().Begin(ctx)
		if err != nil {
			return err
		}
		defer func() { _ = tx.Rollback(ctx) }()
		if _, err := tx.Exec(ctx, `SELECT 1`); err != nil {
			return err
		}
		if err := CompleteTx(ctx, tx, j); err != nil {
			return err
		}
		if j.Args.N == 1 {
			return tx.Commit(ctx)
		}
		return errors.New("rolled back after CompleteTx")
	}))
	c := e.client(t, Config{Workers: workers})
	committed, err := c.Insert(ctx, countArgs{N: 1}, nil)
	require.NoError(t, err)
	r, err := c.JobWork(ctx, committed.Job.ID)
	require.NoError(t, err)
	assert.Equal(t, StateCompleted, r.State)
	assert.Equal(t, 1, r.Attempt)

	rolled, err := c.Insert(ctx, countArgs{N: 2}, nil)
	require.NoError(t, err)
	r, err = c.JobWork(ctx, rolled.Job.ID)
	require.NoError(t, err)
	assert.Equal(t, StateRetryable, r.State, "error after CompleteTx: the rolled back tx left it running, retry applies")

	_, err = c.JobWork(ctx, committed.Job.ID)
	require.ErrorIs(t, err, ErrInvalidTransition, "only available jobs")
	other, err := c.Insert(ctx, flakyArgs{}, nil)
	require.NoError(t, err)
	_, err = c.JobWork(ctx, other.Job.ID)
	require.ErrorIs(t, err, ErrNoWorker)
	require.ErrorIs(t, CompleteTx[countArgs](ctx, nil, &Job[countArgs]{}), ErrNotInWorker)
	Heartbeat(ctx) // no-op outside Work
}

type recordingMiddleware struct {
	MiddlewareDefaults
	mu     sync.Mutex
	events []string
}

func (m *recordingMiddleware) InsertMany(ctx context.Context, p []InsertParams,
	next func(context.Context) ([]*InsertResult, error),
) ([]*InsertResult, error) {
	m.mu.Lock()
	m.events = append(m.events, fmt.Sprintf("insert %d", len(p)))
	m.mu.Unlock()
	return next(ctx)
}

func (m *recordingMiddleware) Work(ctx context.Context, j *JobRow, next func(context.Context) error) error {
	m.mu.Lock()
	m.events = append(m.events, "work "+j.Kind)
	m.mu.Unlock()
	return next(ctx)
}

type overrideHandler struct{}

func (overrideHandler) HandleError(context.Context, *JobRow, error) *ErrorHandlerResult {
	return &ErrorHandlerResult{SetDiscarded: true}
}

func (overrideHandler) HandlePanic(context.Context, *JobRow, any, string) *ErrorHandlerResult {
	return &ErrorHandlerResult{SetCancelled: true}
}

func TestClient_MiddlewareErrorHandlerAndPollOnly(t *testing.T) {
	t.Parallel()
	e := newIT(t)
	ctx := context.Background()
	mw := &recordingMiddleware{}
	workers := NewWorkers()
	AddWorker(workers, WorkFunc[flakyArgs](func(context.Context, *Job[flakyArgs]) error { return errors.New("x") }))
	AddWorker(workers, WorkFunc[countArgs](func(context.Context, *Job[countArgs]) error { panic("p") }))
	c := e.started(t, Config{
		Queues: map[string]QueueConfig{"default": fastQueue(2)}, Workers: workers, PollOnly: true,
		Middleware: []Middleware{mw}, ErrorHandler: overrideHandler{},
	})
	a, err := c.Insert(ctx, flakyArgs{}, nil)
	require.NoError(t, err)
	b, err := c.Insert(ctx, countArgs{}, nil)
	require.NoError(t, err)
	waitState(t, c, a.Job.ID, StateDiscarded)
	waitState(t, c, b.Job.ID, StateCancelled)
	mw.mu.Lock()
	defer mw.mu.Unlock()
	assert.Contains(t, mw.events, "insert 1")
	assert.Contains(t, mw.events, "work flaky")
	assert.Contains(t, mw.events, "work count")
	assert.Nil(t, c.run.notifier, "PollOnly runs no LISTEN connection")
}

func TestClient_StartStopLifecycle(t *testing.T) {
	t.Parallel()
	e := newIT(t)
	ctx := context.Background()
	release := make(chan struct{})
	var finished atomic.Int32
	workers := NewWorkers()
	AddWorker(workers, WorkFunc[countArgs](func(ctx context.Context, j *Job[countArgs]) error {
		if j.Args.N == 2 { // ignores release: only cancellation ends it
			<-ctx.Done()
			return ctx.Err()
		}
		select {
		case <-release:
			finished.Add(1)
			return nil
		case <-ctx.Done():
			return ctx.Err()
		}
	}))
	c := e.client(t, Config{Queues: map[string]QueueConfig{"default": fastQueue(2)}, Workers: workers})
	require.ErrorIs(t, c.Stop(ctx), ErrNotStarted)
	require.NoError(t, c.Start(ctx))
	require.ErrorIs(t, c.Start(ctx), ErrAlreadyStarted)
	res, err := c.Insert(ctx, countArgs{}, nil)
	require.NoError(t, err)
	waitState(t, c, res.Job.ID, StateRunning)

	// Graceful stop waits for the running job.
	stopped := make(chan error, 1)
	go func() { stopped <- c.Stop(context.Background()) }()
	close(release)
	require.NoError(t, <-stopped)
	assert.Equal(t, int32(1), finished.Load())
	waitState(t, c, res.Job.ID, StateCompleted)

	// Stop with a deadline cancels jobs that do not finish in time.
	require.NoError(t, c.Start(ctx))
	res, err = c.Insert(ctx, countArgs{N: 2}, nil)
	require.NoError(t, err)
	waitState(t, c, res.Job.ID, StateRunning)
	sctx, cancel := context.WithTimeout(ctx, 200*time.Millisecond)
	defer cancel()
	err = c.Stop(sctx)
	require.ErrorIs(t, err, context.DeadlineExceeded)
	r := waitState(t, c, res.Job.ID, StateRetryable)
	assert.Contains(t, r.Errors[0].Error, "context canceled")
}

func TestClient_StartNeedsMigratedSchema(t *testing.T) {
	t.Parallel()
	pool := testDB.Pool(t)
	c, err := NewClient(pool, Config{Schema: "never_migrated", Queues: map[string]QueueConfig{"q": {}}})
	require.NoError(t, err)
	require.ErrorIs(t, c.Start(context.Background()), ErrSchemaNotMigrated)
}

func TestClient_AdminViews(t *testing.T) {
	t.Parallel()
	e := newIT(t)
	ctx := context.Background()
	c := e.client(t, Config{})
	for i := 0; i < 5; i++ {
		_, err := c.Insert(ctx, countArgs{N: i}, &InsertOpts{Queue: "q1"})
		require.NoError(t, err)
	}
	page, err := c.JobList(ctx, &JobListParams{Limit: 2, Kinds: []string{"count"}})
	require.NoError(t, err)
	require.Len(t, page.Jobs, 2)
	require.NotEmpty(t, page.NextCursor)
	page2, err := c.JobList(ctx, &JobListParams{Limit: 10, Cursor: page.NextCursor})
	require.NoError(t, err)
	assert.Len(t, page2.Jobs, 3)
	assert.Empty(t, page2.NextCursor)
	_, err = c.JobList(ctx, &JobListParams{Cursor: "garbage"})
	require.ErrorIs(t, err, ErrInvalidArgument)
	big, err := c.JobList(ctx, &JobListParams{Limit: 10_000})
	require.NoError(t, err)
	assert.Len(t, big.Jobs, 5)

	n, err := c.JobDiscardMany(ctx, &JobFilter{Queues: []string{"q1"}})
	require.NoError(t, err)
	assert.Equal(t, 5, n)
	n, err = c.JobRetryMany(ctx, &JobFilter{States: []State{StateDiscarded}})
	require.NoError(t, err)
	assert.Equal(t, 5, n)
	n, err = c.JobDeleteMany(ctx, &JobFilter{Kinds: []string{"count"}})
	require.NoError(t, err)
	assert.Equal(t, 5, n)
	_, err = c.JobDeleteMany(ctx, nil)
	require.ErrorIs(t, err, ErrInvalidArgument)
	_, err = c.LeaderGet(ctx)
	require.ErrorIs(t, err, ErrNotFound)
	ps, err := c.PeriodicList(ctx)
	require.NoError(t, err)
	assert.Empty(t, ps)
}
