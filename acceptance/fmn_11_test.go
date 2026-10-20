// SPDX-License-Identifier: Apache-2.0

//go:build acceptance

package acceptance

import (
	"context"
	"math/rand/v2"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/thnhdatle/foreman"
)

type longArgs struct {
	N int `json:"n"`
}

func (longArgs) Kind() string { return "acc_long" }

// FMN-11: JobCancel on running jobs cancels their context within 1 s (p99)
// and every job ends cancelled.
func TestAcceptance_FMN11_Cancel(t *testing.T) {
	th := loadThresholds(t, "FMN-11")
	total, clients := th.Int(t, "jobs"), th.Int(t, "clients")
	ctx := context.Background()
	const schema = "acc_fmn11"
	pool := newPool(t, dbURL(), 80)
	freshSchema(t, pool, schema)

	var doneAt sync.Map // job id -> time the job's context was cancelled
	started := make(chan int64, total)
	workers := foreman.NewWorkers()
	foreman.AddWorker(workers, foreman.WorkFunc[longArgs](func(ctx context.Context, j *foreman.Job[longArgs]) error {
		started <- j.ID
		select {
		case <-ctx.Done():
			doneAt.Store(j.ID, time.Now())
			return ctx.Err()
		case <-time.After(2 * time.Minute):
			return nil
		}
	}))
	perClient := 1 + total/(2*clients)
	for i := 0; i < clients; i++ {
		c, err := foreman.NewClient(pool, foreman.Config{
			Schema: schema, Workers: workers, Logger: testLogger(t),
			Queues: map[string]foreman.QueueConfig{"default": {MaxWorkers: perClient, FetchInterval: 200 * time.Millisecond}},
		})
		require.NoError(t, err)
		require.NoError(t, c.Start(ctx))
		t.Cleanup(func() {
			sctx, cancel := context.WithTimeout(context.Background(), time.Minute)
			defer cancel()
			_ = c.StopAndCancel(sctx)
		})
	}
	admin, err := foreman.NewClient(pool, foreman.Config{Schema: schema, Logger: testLogger(t)})
	require.NoError(t, err)
	params := make([]foreman.InsertParams, total)
	for i := range params {
		params[i] = foreman.InsertParams{Args: longArgs{N: i}}
	}
	_, err = admin.InsertMany(ctx, params)
	require.NoError(t, err)

	// Cancel each job at a random time after it started running.
	var mu sync.Mutex
	cancelAt := map[int64]time.Time{}
	var wg sync.WaitGroup
	rng := rand.New(rand.NewPCG(11, 11))
	cancelErrs := make(chan error, total)
	for i := 0; i < total; i++ {
		select {
		case id := <-started:
			delay := time.Duration(rng.IntN(500)) * time.Millisecond
			wg.Add(1)
			go func() {
				defer wg.Done()
				<-time.After(delay) // the random cancel time
				mu.Lock()
				cancelAt[id] = time.Now()
				mu.Unlock()
				if _, err := admin.JobCancel(ctx, id); err != nil {
					cancelErrs <- err
				}
			}()
		case <-time.After(5 * time.Minute):
			t.Fatalf("only %d of %d jobs started", i, total)
		}
	}
	wg.Wait()
	close(cancelErrs)
	for err := range cancelErrs {
		require.NoError(t, err)
	}
	require.True(t, waitFor(2*time.Minute, 100*time.Millisecond, func() bool {
		return countWhere(t, pool, schema, "state = 'cancelled'") == int64(total)
	}), "all jobs cancelled")

	latencies := make([]time.Duration, 0, total)
	for id, at := range cancelAt {
		if d, ok := doneAt.Load(id); ok {
			latencies = append(latencies, d.(time.Time).Sub(at))
		}
	}
	cancelled := countWhere(t, pool, schema, "state = 'cancelled'")
	ratio := float64(cancelled) / float64(total)
	p99 := percentile(latencies, 99)
	measured := map[string]any{
		"jobs": total, "cancelled": cancelled, "cancelled_ratio": ratio, "observed": len(latencies),
		"cancel_latency_p50_ms": ms(percentile(latencies, 50)), "cancel_latency_p99_ms": ms(p99),
		"cancel_latency_max_ms": ms(percentile(latencies, 100)),
	}
	maxP99 := time.Duration(th.Int(t, "cancel_latency_p99_ms_max")) * time.Millisecond
	pass := len(latencies) == total && p99 <= maxP99 && ratio >= th.Float(t, "cancelled_ratio_min")
	writeResult(t, "FMN-11", measured, pass, nil)
	assert.Len(t, latencies, total, "every job saw its context cancelled")
	assert.LessOrEqual(t, p99, maxP99)
	assert.GreaterOrEqual(t, ratio, th.Float(t, "cancelled_ratio_min"))
}
