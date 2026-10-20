// SPDX-License-Identifier: Apache-2.0

//go:build integration

package pgstore_test

import (
	"context"
	"fmt"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/thnhdatle/foreman/internal/domain/job"
)

func fetch(t *testing.T, e *env, p job.FetchParams) []*job.Row {
	t.Helper()
	if p.Now.IsZero() {
		p.Now = t0
	}
	if p.ClientID == "" {
		p.ClientID = "c1"
	}
	rows, err := e.store.Fetch(context.Background(), p)
	require.NoError(t, err)
	return rows
}

func TestFetch_PlainOrderAndRunningState(t *testing.T) {
	t.Parallel()
	e := newEnv(t)
	var ps []job.InsertParams
	for i, prio := range []int{3, 1, 2, 1, 4} {
		p := params("k", "q")
		p.Priority = prio
		p.ScheduledAt = t0.Add(time.Duration(i) * time.Second)
		ps = append(ps, p)
	}
	sched := params("k", "q")
	sched.State, sched.ScheduledAt = job.StateScheduled, t0.Add(time.Hour)
	ps = append(ps, sched, params("k", "other"))
	ids := []int64{}
	for _, r := range e.insert(t, ps...) {
		ids = append(ids, r.Row.ID)
	}
	now := t0.Add(time.Minute)
	rows := fetch(t, e, job.FetchParams{Queue: "q", ClientID: "client-a", Limit: 3, Now: now})
	require.Len(t, rows, 3)
	assert.Equal(t, []int64{ids[1], ids[3], ids[2]}, []int64{rows[0].ID, rows[1].ID, rows[2].ID}, "priority, then scheduled_at")
	for _, r := range rows {
		assert.Equal(t, job.StateRunning, r.State)
		assert.Equal(t, 1, r.Attempt)
		assert.Equal(t, []string{"client-a"}, r.AttemptedBy)
		require.NotNil(t, r.AttemptedAt)
		assert.True(t, now.Equal(*r.AttemptedAt))
		assert.True(t, now.Equal(*r.HeartbeatAt))
	}
	rest := fetch(t, e, job.FetchParams{Queue: "q", Limit: 10})
	assert.Len(t, rest, 2, "scheduled and other-queue jobs are not fetched")
	assert.Empty(t, fetch(t, e, job.FetchParams{Queue: "q", Limit: 10}))
	assert.Empty(t, fetch(t, e, job.FetchParams{Queue: "q", Limit: 0}))
}

func TestFetch_AttemptedByKeepsLast100(t *testing.T) {
	t.Parallel()
	e := newEnv(t)
	id := e.insert(t, params("k", "q"))[0].Row.ID
	hist := make([]string, 120)
	for i := range hist {
		hist[i] = fmt.Sprintf("c%d", i)
	}
	e.exec(t, `UPDATE "`+e.schema+`".foreman_jobs SET attempted_by = $1 WHERE id = $2`, hist, id)
	rows := fetch(t, e, job.FetchParams{Queue: "q", ClientID: "last", Limit: 1})
	require.Len(t, rows, 1)
	assert.Len(t, rows[0].AttemptedBy, 100)
	assert.Equal(t, "last", rows[0].AttemptedBy[99])
	assert.Equal(t, "c21", rows[0].AttemptedBy[0])
}

func TestFetch_PausedQueueFetchesNothing(t *testing.T) {
	t.Parallel()
	e := newEnv(t)
	ctx := context.Background()
	e.insert(t, func() job.InsertParams { p := params("k", "q"); p.ConcurrencyKey = "a"; return p }(), params("k", "q"))
	require.NoError(t, e.store.QueueSetPaused(ctx, "q", true, t0))
	paused, err := e.store.QueueHeartbeat(ctx, "q", t0)
	require.NoError(t, err)
	assert.True(t, paused)
	assert.Empty(t, fetch(t, e, job.FetchParams{Queue: "q", Limit: 5}))
	assert.Empty(t, fetch(t, e, job.FetchParams{Queue: "q", Keyed: true, Quotas: []job.KeyQuota{{Key: "a", Quota: 5}, {Key: "", Quota: 5}}}))
	assert.Empty(t, fetch(t, e, job.FetchParams{Queue: "q", Keyed: true, PerKeyLimit: 2, Quotas: []job.KeyQuota{{Key: "a", Quota: 5}}}))
	require.NoError(t, e.store.QueueSetPaused(ctx, "q", false, t0))
	paused, err = e.store.QueueHeartbeat(ctx, "q", t0.Add(time.Second))
	require.NoError(t, err)
	assert.False(t, paused)
	assert.Len(t, fetch(t, e, job.FetchParams{Queue: "q", Limit: 5}), 2)
	served, err := e.store.ServedQueues(ctx, t0.Add(time.Second))
	require.NoError(t, err)
	assert.Equal(t, []string{"q"}, served)
}

func keyed(key string, n int) []job.InsertParams {
	out := make([]job.InsertParams, n)
	for i := range out {
		out[i] = params("k", "q")
		out[i].ConcurrencyKey = key
		out[i].ScheduledAt = t0.Add(time.Duration(i) * time.Millisecond)
	}
	return out
}

func TestKeyStats_LooseIndexScan(t *testing.T) {
	t.Parallel()
	e := newEnv(t)
	ctx := context.Background()
	e.insert(t, keyed("b", 3)...)
	e.insert(t, keyed("a", 2)...)
	e.insert(t, keyed("", 2)...)
	high := keyed("c", 1)
	high[0].Priority = 1
	e.insert(t, high...)
	fetch(t, e, job.FetchParams{Queue: "q", Keyed: true, Quotas: []job.KeyQuota{{Key: "b", Quota: 1}}})

	stats, err := e.store.KeyStats(ctx, "q", 100, true)
	require.NoError(t, err)
	require.Len(t, stats, 4)
	assert.Equal(t, []string{"a", "b", "c", ""}, []string{stats[0].Key, stats[1].Key, stats[2].Key, stats[3].Key})
	assert.Equal(t, 1, stats[1].Running)
	assert.Equal(t, 0, stats[0].Running)
	assert.Equal(t, 1, stats[2].Priority)
	assert.True(t, stats[1].ScheduledAt.Equal(t0.Add(time.Millisecond)), "head job of b after fetch")

	limited, err := e.store.KeyStats(ctx, "q", 2, false)
	require.NoError(t, err)
	assert.Len(t, limited, 3, "limit applies to keyed entries; the no-key bucket is always listed")
	assert.Zero(t, limited[1].Running)
}

func TestFetch_KeyedQuotasAndNoKeyBucket(t *testing.T) {
	t.Parallel()
	e := newEnv(t)
	e.insert(t, keyed("a", 5)...)
	e.insert(t, keyed("b", 5)...)
	e.insert(t, keyed("", 5)...)
	rows := fetch(t, e, job.FetchParams{Queue: "q", Keyed: true, Quotas: []job.KeyQuota{
		{Key: "a", Quota: 2}, {Key: "b", Quota: 1}, {Key: "", Quota: 3}, {Key: "missing", Quota: 4},
	}})
	byKey := map[string]int{}
	for _, r := range rows {
		byKey[r.ConcurrencyKey]++
	}
	assert.Equal(t, map[string]int{"a": 2, "b": 1, "": 3}, byKey)
}

func TestFetch_PerKeyLimitCountsRunning(t *testing.T) {
	t.Parallel()
	e := newEnv(t)
	e.insert(t, keyed("a", 10)...)
	first := fetch(t, e, job.FetchParams{Queue: "q", Keyed: true, PerKeyLimit: 3, Quotas: []job.KeyQuota{{Key: "a", Quota: 2}}})
	assert.Len(t, first, 2)
	second := fetch(t, e, job.FetchParams{Queue: "q", Keyed: true, PerKeyLimit: 3, Quotas: []job.KeyQuota{{Key: "a", Quota: 5}}})
	assert.Len(t, second, 1, "only one more fits under the limit")
	assert.Empty(t, fetch(t, e, job.FetchParams{Queue: "q", Keyed: true, PerKeyLimit: 3, Quotas: []job.KeyQuota{{Key: "a", Quota: 5}}}))
	ctx := context.Background()
	_, err := e.store.Complete(ctx, "c1", []job.Completion{{ID: first[0].ID, Attempt: 1, State: job.StateCompleted, FinalizedAt: t0, At: t0}})
	require.NoError(t, err)
	assert.Len(t, fetch(t, e, job.FetchParams{Queue: "q", Keyed: true, PerKeyLimit: 3, Quotas: []job.KeyQuota{{Key: "a", Quota: 5}}}), 1)
}

// Property against Postgres: concurrent producers fetching
// with a per-key limit never have more than the limit running per key.
func TestFetch_ConcurrentProducersNeverExceedPerKeyLimit(t *testing.T) {
	t.Parallel()
	e := newEnv(t)
	ctx := context.Background()
	const keys, perKey, limit, producers = 8, 40, 3, 6
	for k := 0; k < keys; k++ {
		e.insert(t, keyed(fmt.Sprintf("k%d", k), perKey)...)
	}
	var violations atomic.Int32
	var done atomic.Int32
	var wg sync.WaitGroup
	stop := make(chan struct{})
	wg.Add(1)
	go func() { // sampler
		defer wg.Done()
		for {
			select {
			case <-stop:
				return
			default:
			}
			var over int
			err := e.pool.QueryRow(ctx, `SELECT count(*) FROM (SELECT concurrency_key FROM "`+e.schema+`".foreman_jobs
				WHERE state = 'running' GROUP BY concurrency_key HAVING count(*) > $1) x`, limit).Scan(&over)
			if err == nil && over > 0 {
				violations.Add(1)
			}
		}
	}()
	var workers sync.WaitGroup
	for p := 0; p < producers; p++ {
		workers.Add(1)
		go func() {
			defer workers.Done()
			cid := fmt.Sprintf("p%d", p)
			for done.Load() < keys*perKey {
				quotas := make([]job.KeyQuota, keys)
				for k := range quotas {
					quotas[k] = job.KeyQuota{Key: fmt.Sprintf("k%d", (k+p)%keys), Quota: 2}
				}
				rows, err := e.store.Fetch(ctx, job.FetchParams{
					Queue: "q", ClientID: cid, Now: t0, Keyed: true,
					PerKeyLimit: limit, Quotas: quotas,
				})
				if err != nil {
					t.Error(err)
					return
				}
				cs := make([]job.Completion, len(rows))
				for i, r := range rows {
					cs[i] = job.Completion{ID: r.ID, Attempt: r.Attempt, State: job.StateCompleted, FinalizedAt: t0, At: t0}
				}
				applied, err := e.store.Complete(ctx, cid, cs)
				if err != nil {
					t.Error(err)
					return
				}
				done.Add(int32(len(applied)))
			}
		}()
	}
	workers.Wait()
	close(stop)
	wg.Wait()
	assert.Zero(t, violations.Load())
	assert.Equal(t, keys*perKey, e.count(t, "state = 'completed'"))
}

func TestFetch_SkipLockedAcrossConcurrentFetchers(t *testing.T) {
	t.Parallel()
	e := newEnv(t)
	ctx := context.Background()
	var ps []job.InsertParams
	for i := 0; i < 500; i++ {
		ps = append(ps, params("k", "q"))
	}
	e.insert(t, ps...)
	var mu sync.Mutex
	seen := map[int64]int{}
	var wg sync.WaitGroup
	for p := 0; p < 8; p++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for {
				rows, err := e.store.Fetch(ctx, job.FetchParams{Queue: "q", ClientID: fmt.Sprint(p), Limit: 7, Now: t0})
				if err != nil {
					t.Error(err)
					return
				}
				if len(rows) == 0 {
					return
				}
				mu.Lock()
				for _, r := range rows {
					seen[r.ID]++
				}
				mu.Unlock()
			}
		}()
	}
	wg.Wait()
	assert.Len(t, seen, 500)
	for id, n := range seen {
		assert.Equal(t, 1, n, "job %d fetched once", id)
	}
}

// The fetch query uses the fetch index at 1M rows.
func TestFetch_PlanUsesFetchIndexAt1M(t *testing.T) {
	t.Parallel()
	e := newEnv(t)
	ctx := context.Background()
	q := `"` + e.schema + `".foreman_jobs`
	e.exec(t, `INSERT INTO `+q+` (kind, queue, args, state, priority, scheduled_at, created_at, finalized_at)
		SELECT 'k', 'q' || (g % 4), '{}', CASE WHEN g % 10 = 0 THEN 'available' ELSE 'completed' END::"`+e.schema+`".foreman_state,
		1 + g % 4, $1::timestamptz + g * interval '1 ms', $1, CASE WHEN g % 10 = 0 THEN NULL ELSE $1::timestamptz END
		FROM generate_series(1, 1000000) g`, t0)
	e.exec(t, `ANALYZE `+q)
	plan := explainFetch(t, e)
	assert.Contains(t, plan, "foreman_jobs_fetch_idx", plan)
	assert.NotContains(t, plan, "Seq Scan", plan)
	_ = ctx
}

func explainFetch(t *testing.T, e *env) string {
	t.Helper()
	q := `EXPLAIN SELECT id FROM "` + e.schema + `".foreman_jobs WHERE queue = 'q1' AND state = 'available'
		ORDER BY priority, scheduled_at, id LIMIT 50 FOR UPDATE SKIP LOCKED`
	rows, err := e.pool.Query(context.Background(), q)
	require.NoError(t, err)
	defer rows.Close()
	var lines []string
	for rows.Next() {
		var l string
		require.NoError(t, rows.Scan(&l))
		lines = append(lines, l)
	}
	require.NoError(t, rows.Err())
	return strings.Join(lines, "\n")
}
