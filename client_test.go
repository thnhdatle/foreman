// SPDX-License-Identifier: Apache-2.0

package foreman

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/thnhdatle/foreman/internal/domain/job"
	"github.com/thnhdatle/foreman/internal/platform/clock"
)

type unitArgs struct {
	ID int64 `json:"id"`
}

func (unitArgs) Kind() string { return "unit" }

// lazyPool never connects: NewClient and option resolution need no database.
func lazyPool(t *testing.T) *pgxpool.Pool {
	t.Helper()
	pool, err := pgxpool.New(context.Background(), "postgres://nobody@127.0.0.1:1/none?connect_timeout=1")
	require.NoError(t, err)
	t.Cleanup(pool.Close)
	return pool
}

func TestNewClient_DefaultsAndValidation(t *testing.T) {
	t.Parallel()
	_, err := NewClient(nil, Config{})
	require.ErrorIs(t, err, ErrInvalidArgument)

	c, err := NewClient(lazyPool(t), Config{Queues: map[string]QueueConfig{"default": {}}})
	require.NoError(t, err)
	assert.Equal(t, DefaultSchema, c.Schema())
	assert.NotEmpty(t, c.ID())
	assert.NotNil(t, c.Pool())
	assert.False(t, c.IsLeader())
	assert.Equal(t, DefaultMaxWorkers, c.cfg.Queues["default"].MaxWorkers)
	assert.Equal(t, DefaultFetchInterval, c.cfg.Queues["default"].FetchInterval)
	assert.Equal(t, DefaultHeartbeatInterval, c.cfg.HeartbeatInterval)
	assert.Equal(t, DefaultRescueStuckJobsAfter, c.cfg.RescueStuckJobsAfter)
	assert.Equal(t, DefaultCompletedJobRetention, c.cfg.CompletedJobRetention)
	assert.Equal(t, DefaultCancelledJobRetention, c.cfg.CancelledJobRetention)
	assert.Equal(t, DefaultDiscardedJobRetention, c.cfg.DiscardedJobRetention)
	assert.Equal(t, DefaultReindexInterval, c.cfg.ReindexInterval)
	assert.Equal(t, DefaultCleanerInterval, c.cfg.CleanerInterval)
	assert.Equal(t, DefaultMaxArgsBytes, c.cfg.MaxArgsBytes)
	assert.Equal(t, time.Second, c.cfg.minFetchInterval())

	tests := []struct {
		name string
		cfg  Config
		msgs []string
	}{
		{
			"rescue below two heartbeats",
			Config{HeartbeatInterval: time.Minute, RescueStuckJobsAfter: time.Minute},
			[]string{"at least 2 x HeartbeatInterval"},
		},
		{
			"bad queue",
			Config{Queues: map[string]QueueConfig{"no spaces": {}, "neg": {MaxWorkers: -1}}},
			[]string{`queue name "no spaces"`, `queue "neg"`},
		},
		{"negative durations", Config{JobTimeout: -1, MaxArgsBytes: -1}, []string{"must not be negative"}},
		{"long id", Config{ID: string(make([]byte, 201))}, []string{"ID must be"}},
		{"bad schema", Config{Schema: "x;y"}, []string{"invalid schema"}},
		{"periodic", Config{PeriodicJobs: []*PeriodicJob{
			nil,
			{Name: "", Schedule: Every(time.Minute), Args: func() (JobArgs, *InsertOpts) { return unitArgs{}, nil }},
			{Name: "a", Schedule: Every(0), Args: nil, CatchUp: -1},
			{Name: "a"},
		}}, []string{
			"is nil", "has no Name", "no Args func", "CatchUp must not be negative", "interval must be positive",
			"used twice", "has no Schedule",
		}},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			_, err := NewClient(lazyPool(t), tc.cfg)
			require.ErrorIs(t, err, ErrInvalidConfig)
			for _, m := range tc.msgs {
				assert.Contains(t, err.Error(), m)
			}
		})
	}
}

func TestClient_BuildInsert(t *testing.T) {
	t.Parallel()
	now := time.Date(2026, 1, 2, 3, 4, 5, 0, time.UTC)
	workers := NewWorkers()
	AddWorker(workers, WorkFunc[unitArgs](func(context.Context, *Job[unitArgs]) error { return nil }), WithMaxAttempts(3))
	c, err := NewClient(lazyPool(t), Config{Workers: workers, Clock: clock.NewFake(now), MaxArgsBytes: 40})
	require.NoError(t, err)

	p, err := c.buildInsert(unitArgs{ID: 9007199254740993}, nil, now)
	require.NoError(t, err)
	assert.Equal(t, "unit", p.Kind)
	assert.JSONEq(t, `{"id":9007199254740993}`, string(p.Args))
	assert.Equal(t, job.StateAvailable, p.State)
	assert.Equal(t, now, p.ScheduledAt)
	assert.Equal(t, 3, p.MaxAttempts)
	assert.Nil(t, p.UniqueIdentity)

	p, err = c.buildInsert(unitArgs{ID: 1}, &InsertOpts{
		ScheduledAt: now.Add(time.Minute), UniqueOpts: &UniqueOpts{ByArgs: true, ByState: []State{
			StateAvailable, StateScheduled, StateRunning, StateRetryable, StateDiscarded,
		}},
	}, now)
	require.NoError(t, err)
	assert.Equal(t, job.StateScheduled, p.State)
	assert.JSONEq(t, `{"kind":"unit","args":{"id":1}}`, string(p.UniqueIdentity))
	assert.Equal(t, "11110010", p.UniqueStates)

	p, err = c.buildInsert(unitArgs{ID: 1}, &InsertOpts{ScheduledAt: now.Add(-time.Hour)}, now)
	require.NoError(t, err)
	assert.Equal(t, now, p.ScheduledAt, "past schedules run now")

	_, err = c.buildInsert(unitArgs{ID: 1}, &InsertOpts{UniqueOpts: &UniqueOpts{ByPeriod: time.Millisecond}}, now)
	require.ErrorIs(t, err, ErrInvalidInsertOpts)
	_, err = c.buildInsert(bigArgs{}, nil, now)
	require.ErrorIs(t, err, ErrArgsTooLarge)
	_, err = c.buildInsert(badArgs{}, nil, now)
	require.ErrorContains(t, err, "encode")
	_, err = c.buildInsert(unitArgs{}, &InsertOpts{
		Tags: []string{""}, ConcurrencyKey: string(make([]byte, 300)), Timeout: -time.Second,
	}, now)
	require.ErrorIs(t, err, ErrInvalidInsertOpts)
	for _, m := range []string{"tag", "concurrency key", "timeout"} {
		assert.Contains(t, err.Error(), m)
	}
	_, err = c.buildInsert(badKind{}, nil, now)
	require.ErrorContains(t, err, "kind")
}

type bigArgs struct {
	S string `json:"s"`
}

func (bigArgs) Kind() string { return "big" }

func (bigArgs) InsertOpts() InsertOpts { return InsertOpts{} }

func (b bigArgs) MarshalJSON() ([]byte, error) {
	return json.Marshal(map[string]string{"s": string(make([]byte, 100))})
}

type badArgs struct{}

func (badArgs) Kind() string                 { return "bad" }
func (badArgs) MarshalJSON() ([]byte, error) { return nil, assert.AnError }

type badKind struct{}

func (badKind) Kind() string { return "has space" }

func TestWorkers(t *testing.T) {
	t.Parallel()
	w := NewWorkers()
	AddWorker(w, WorkFunc[unitArgs](func(context.Context, *Job[unitArgs]) error { return nil }),
		WithTimeout(time.Second), WithRetryPolicy(ConstantBackoff{Delay: time.Second}))
	assert.Equal(t, []string{"unit"}, w.Kinds())
	assert.Equal(t, time.Second, w.byKind["unit"].timeout)
	assert.Panics(t, func() {
		AddWorker(w, WorkFunc[unitArgs](func(context.Context, *Job[unitArgs]) error { return nil }))
	})
	assert.Panics(t, func() {
		AddWorker(w, WorkFunc[emptyKind](func(context.Context, *Job[emptyKind]) error { return nil }))
	})
	err := w.byKind["unit"].work(context.Background(), &JobRow{ID: 1, Kind: "unit", Args: []byte(`not json`)})
	require.ErrorContains(t, err, "decode args")
	var got *Job[unitArgs]
	w2 := NewWorkers()
	AddWorker(w2, WorkFunc[unitArgs](func(_ context.Context, j *Job[unitArgs]) error { got = j; return nil }))
	row := &JobRow{
		ID: 5, Kind: "unit", Args: []byte(`{"id":7}`), Attempt: 2, MaxAttempts: 3, Queue: "q", Priority: 1,
		Errors: []AttemptError{{Error: "x"}},
	}
	require.NoError(t, w2.byKind["unit"].work(context.Background(), row))
	assert.Equal(t, int64(7), got.Args.ID)
	assert.Equal(t, 2, got.Attempt)
	assert.Equal(t, "x", got.Errors[0].Error)
}

type emptyKind struct{}

func (emptyKind) Kind() string { return "" }

func TestDecodeArgs(t *testing.T) {
	t.Parallel()
	_, err := DecodeArgs[unitArgs](nil)
	require.ErrorIs(t, err, ErrInvalidArgument)
	_, err = DecodeArgs[unitArgs](&JobRow{Kind: "unit", Args: []byte(`{`)})
	require.ErrorContains(t, err, "decode args")
	a, err := DecodeArgs[unitArgs](&JobRow{Kind: "unit", Args: []byte(`{"id":3}`)})
	require.NoError(t, err)
	assert.Equal(t, int64(3), a.ID)
}

func TestRetryPolicies(t *testing.T) {
	t.Parallel()
	now := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	for attempt := 1; attempt < 30; attempt++ {
		next := ExponentialBackoff{}.NextRetry(JobRow{Attempt: attempt}, now)
		assert.False(t, next.Before(now))
		assert.False(t, next.After(now.Add(24*time.Hour)))
	}
	next := ExponentialBackoff{Base: time.Millisecond, Cap: 2 * time.Millisecond}.NextRetry(JobRow{Attempt: 10}, now)
	assert.LessOrEqual(t, next.Sub(now), 2*time.Millisecond)
	assert.Equal(t, now.Add(time.Minute), ConstantBackoff{Delay: time.Minute}.NextRetry(JobRow{}, now))
	assert.Equal(t, now, ConstantBackoff{Delay: -time.Minute}.NextRetry(JobRow{}, now))
	f := RetryPolicyFunc(func(j JobRow, n time.Time) time.Time { return n.Add(time.Duration(j.Attempt) * time.Second) })
	assert.Equal(t, now.Add(3*time.Second), f.NextRetry(JobRow{Attempt: 3}, now))
}

func TestSchedules(t *testing.T) {
	t.Parallel()
	_, err := Cron("bogus")
	require.Error(t, err)
	assert.Panics(t, func() { MustCron("bogus") })
	s := MustCron("0 * * * *")
	t0 := time.Date(2026, 1, 1, 0, 30, 0, 0, time.UTC)
	assert.Equal(t, time.Date(2026, 1, 1, 1, 0, 0, 0, time.UTC), s.Next(t0))
	assert.Equal(t, time.Date(2026, 1, 1, 0, 45, 0, 0, time.UTC), Every(15*time.Minute).Next(t0))
	assert.True(t, Every(0).Next(t0).IsZero())

	ny, err := time.LoadLocation("America/New_York")
	require.NoError(t, err)
	pj := &PeriodicJob{Name: "n", Schedule: MustCron("0 9 * * *"), TimeZone: ny}
	rs, err := pj.resolvedSchedule()
	require.NoError(t, err)
	assert.Equal(t, time.Date(2026, 1, 1, 14, 0, 0, 0, time.UTC), rs.Next(t0), "09:00 EST is 14:00 UTC")
	custom := &PeriodicJob{Schedule: Every(time.Hour)}
	_, err = custom.resolvedSchedule()
	require.NoError(t, err)
	other := &PeriodicJob{Schedule: fixedSchedule{}}
	rs, err = other.resolvedSchedule()
	require.NoError(t, err)
	assert.Equal(t, fixedSchedule{}, rs)
}

type fixedSchedule struct{}

func (fixedSchedule) Next(t time.Time) time.Time { return t.Add(time.Hour) }

func TestCursor(t *testing.T) {
	t.Parallel()
	id, err := decodeCursor(encodeCursor(42))
	require.NoError(t, err)
	assert.Equal(t, int64(42), id)
	id, err = decodeCursor("")
	require.NoError(t, err)
	assert.Zero(t, id)
	for _, bad := range []string{"!!", "aWQ6", "aWQ6eA", "eDox"} {
		_, err = decodeCursor(bad)
		require.ErrorIs(t, err, ErrInvalidArgument, bad)
	}
}

func TestResultHelpers(t *testing.T) {
	t.Parallel()
	require.ErrorContains(t, Snooze(time.Second), "snoozed")
	require.ErrorContains(t, Discard(assert.AnError), "discarded")
	require.ErrorContains(t, Cancel(assert.AnError), "cancelled")
	assert.Nil(t, ClientFromContext(context.Background()))
	assert.Nil(t, toJobRow(nil))
	assert.Nil(t, toOverride(nil))
	assert.Equal(t, &job.Override{SnoozeFor: time.Second}, toOverride(&ErrorHandlerResult{SnoozeFor: time.Second}))
	assert.Equal(t, 5, firstNonZero(0, 5, 7))
	assert.Empty(t, firstNonZero("", ""))
}

func TestMiddlewareDefaults(t *testing.T) {
	t.Parallel()
	var m MiddlewareDefaults
	called := 0
	res, err := m.InsertMany(context.Background(), nil, func(context.Context) ([]*InsertResult, error) {
		called++
		return nil, nil
	})
	require.NoError(t, err)
	assert.Nil(t, res)
	require.NoError(t, m.Work(context.Background(), &JobRow{}, func(context.Context) error { called++; return nil }))
	assert.Equal(t, 2, called)
}
