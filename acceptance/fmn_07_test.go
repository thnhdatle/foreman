// SPDX-License-Identifier: Apache-2.0

//go:build acceptance

package acceptance

import (
	"context"
	"fmt"
	"math/rand/v2"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/thnhdatle/foreman"
)

type keyedArgs struct {
	N       int `json:"n"`
	SleepMS int `json:"sleep_ms"`
}

func (keyedArgs) Kind() string { return "acc_keyed" }

// FMN-07: a per-key limit of 5 is never exceeded, measured in the database
// and in the workers every 10 ms, with 4 clients and 1,000 keys.
func TestAcceptance_FMN07_PerKeyLimit(t *testing.T) {
	th := loadThresholds(t, "FMN-07")
	total, keys, clients := th.Int(t, "jobs"), th.Int(t, "keys"), th.Int(t, "clients")
	workersPer, limit := th.Int(t, "workers_per_client"), th.Int(t, "per_key_limit")
	sample := time.Duration(th.Int(t, "sample_interval_ms")) * time.Millisecond
	ctx := context.Background()
	const schema = "acc_fmn07"
	pool := newPool(t, dbURL(), 100)
	freshSchema(t, pool, schema)

	var mu sync.Mutex
	active := map[string]int{}
	var workerViolations, completed atomic.Int64
	workers := foreman.NewWorkers()
	foreman.AddWorker(workers, foreman.WorkFunc[keyedArgs](func(ctx context.Context, j *foreman.Job[keyedArgs]) error {
		key := fmt.Sprintf("tenant-%d", j.Args.N%keys)
		mu.Lock()
		active[key]++
		if active[key] > limit {
			workerViolations.Add(1)
		}
		mu.Unlock()
		defer func() {
			mu.Lock()
			active[key]--
			mu.Unlock()
			completed.Add(1)
		}()
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(time.Duration(j.Args.SleepMS) * time.Millisecond):
			return nil
		}
	}))
	admin, err := foreman.NewClient(pool, foreman.Config{Schema: schema, Logger: testLogger(t)})
	require.NoError(t, err)
	// Skewed keys: a few hot tenants hold many jobs, so the limit binds.
	rng := rand.New(rand.NewPCG(7, 7))
	params := make([]foreman.InsertParams, total)
	for i := range params {
		n := i
		if rng.IntN(2) == 0 {
			n = i - i%keys + rng.IntN(10) // one of 10 hot keys
		}
		params[i] = foreman.InsertParams{
			Args:       keyedArgs{N: n, SleepMS: 1 + rng.IntN(20)},
			InsertOpts: &foreman.InsertOpts{ConcurrencyKey: fmt.Sprintf("tenant-%d", n%keys)},
		}
	}
	for start := 0; start < total; start += 5000 {
		_, err := admin.InsertMany(ctx, params[start:min(start+5000, total)])
		require.NoError(t, err)
	}

	stop := make(chan struct{})
	var dbViolations, samples, maxRunning atomic.Int64
	var samplerWG sync.WaitGroup
	samplerWG.Add(1)
	go func() {
		defer samplerWG.Done()
		tick := time.NewTicker(sample)
		defer tick.Stop()
		for {
			select {
			case <-stop:
				return
			case <-tick.C:
			}
			var over, top int64
			err := pool.QueryRow(ctx, `SELECT count(*) FILTER (WHERE n > $1), coalesce(max(n), 0) FROM (
				SELECT count(*) AS n FROM "acc_fmn07".foreman_jobs WHERE state = 'running' GROUP BY concurrency_key) x`,
				limit).Scan(&over, &top)
			if err == nil {
				samples.Add(1)
				dbViolations.Add(over)
				if top > maxRunning.Load() {
					maxRunning.Store(top)
				}
			}
		}
	}()

	start := time.Now()
	for i := 0; i < clients; i++ {
		c, err := foreman.NewClient(pool, foreman.Config{
			Schema: schema, Workers: workers, Logger: testLogger(t),
			Queues: map[string]foreman.QueueConfig{"default": {
				MaxWorkers: workersPer, PerKeyLimit: limit, FetchInterval: 100 * time.Millisecond,
			}},
		})
		require.NoError(t, err)
		require.NoError(t, c.Start(ctx))
		t.Cleanup(func() {
			sctx, cancel := context.WithTimeout(context.Background(), time.Minute)
			defer cancel()
			_ = c.StopAndCancel(sctx)
		})
	}
	finished := waitFor(30*time.Minute, 200*time.Millisecond, func() bool {
		return countWhere(t, pool, schema, "state = 'completed'") == int64(total)
	})
	elapsed := time.Since(start)
	close(stop)
	samplerWG.Wait()

	violations := dbViolations.Load() + workerViolations.Load()
	measured := map[string]any{
		"jobs": total, "keys": keys, "clients": clients, "per_key_limit": limit, "completed": completed.Load(),
		"db_samples": samples.Load(), "db_violations": dbViolations.Load(), "worker_violations": workerViolations.Load(),
		"violations": violations, "max_running_per_key": maxRunning.Load(), "elapsed_s": elapsed.Seconds(),
		"jobs_per_s": float64(total) / elapsed.Seconds(),
	}
	pass := finished && violations <= int64(th.Int(t, "violations_max"))
	writeResult(t, "FMN-07", measured, pass, nil)
	require.True(t, finished, "all jobs completed")
	assert.LessOrEqual(t, violations, int64(th.Int(t, "violations_max")))
	assert.Positive(t, samples.Load())
}
