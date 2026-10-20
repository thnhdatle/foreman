// SPDX-License-Identifier: Apache-2.0

//go:build acceptance

package acceptance

import (
	"context"
	"math"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/thnhdatle/foreman"
)

type soakSample struct {
	At        float64 `json:"at_s"`
	SizeBytes int64   `json:"size_bytes"`
	P99MS     float64 `json:"p99_ms"`
	Rows      int64   `json:"rows"`
}

// FMN-12: a soak at 1,000 jobs/s with 5-minute retention, a 1 s cleaner and
// a 10-minute reindex keeps table + index size and p99 pickup latency stable:
// the value at half time is compared with the value at the end.
func TestAcceptance_FMN12_Soak(t *testing.T) {
	th := loadThresholds(t, "FMN-12")
	d := time.Duration(th.Int(t, "duration_s")) * time.Second
	rate := th.Int(t, "rate_per_s")
	ctx := context.Background()
	const schema = "acc_fmn12"
	pool := newPool(t, dbURL(), 120)
	freshSchema(t, pool, schema)
	const clients, workers = 4, 50
	cs := make([]*foreman.Client, clients)
	for i := range cs {
		w := foreman.NewWorkers()
		foreman.AddWorker(w, foreman.WorkFunc[noopArgs](func(context.Context, *foreman.Job[noopArgs]) error { return nil }))
		c, err := foreman.NewClient(pool, foreman.Config{
			Schema: schema, Workers: w, Logger: testLogger(t),
			Queues:                map[string]foreman.QueueConfig{"default": {MaxWorkers: workers}},
			CompletedJobRetention: time.Duration(th.Int(t, "completed_retention_s")) * time.Second,
			CleanerInterval:       time.Duration(th.Int(t, "cleaner_interval_s")) * time.Second,
			ReindexInterval:       time.Duration(th.Int(t, "reindex_interval_s")) * time.Second,
		})
		require.NoError(t, err)
		require.NoError(t, c.Start(ctx))
		cs[i] = c
	}
	defer stopAll(cs)
	producer, err := foreman.NewClient(pool, foreman.Config{Schema: schema, Logger: testLogger(t)})
	require.NoError(t, err)

	start := time.Now()
	var samples []soakSample
	var mu sync.Mutex
	stop := make(chan struct{})
	var samplerWG sync.WaitGroup
	samplerWG.Add(1)
	go func() {
		defer samplerWG.Done()
		tick := time.NewTicker(time.Minute)
		defer tick.Stop()
		for {
			select {
			case <-stop:
				return
			case now := <-tick.C:
				var s soakSample
				s.At = now.Sub(start).Seconds()
				// Jobs created 70 to 10 seconds ago have all been picked up.
				err := pool.QueryRow(ctx, `SELECT pg_total_relation_size('"acc_fmn12".foreman_jobs'),
					coalesce((SELECT percentile_cont(0.99) WITHIN GROUP (ORDER BY extract(epoch FROM attempted_at - created_at))
					 FROM "acc_fmn12".foreman_jobs WHERE created_at BETWEEN $1 AND $2 AND attempted_at IS NOT NULL), 0) * 1000,
					(SELECT count(*) FROM "acc_fmn12".foreman_jobs)`,
					now.Add(-70*time.Second), now.Add(-10*time.Second)).Scan(&s.SizeBytes, &s.P99MS, &s.Rows)
				if err == nil {
					mu.Lock()
					samples = append(samples, s)
					mu.Unlock()
				}
			}
		}
	}()
	inserted, err := steadyInserts(producer, rate, d)
	close(stop)
	samplerWG.Wait()
	require.NoError(t, err)

	mu.Lock()
	defer mu.Unlock()
	require.GreaterOrEqual(t, len(samples), 4, "enough samples")
	half := samples[len(samples)/2-1]
	last := samples[len(samples)-1]
	// Latency is compared over the 5 samples (minutes) ending at each point.
	window := func(end int) float64 {
		sum, n := 0.0, 0
		for i := max(0, end-4); i <= end; i++ {
			sum += samples[i].P99MS
			n++
		}
		return sum / float64(n)
	}
	p99Half, p99Last := window(len(samples)/2-1), window(len(samples)-1)
	sizeDrift := math.Abs(float64(half.SizeBytes-last.SizeBytes)) / float64(last.SizeBytes)
	p99Drift := (p99Last - p99Half) / p99Half
	measured := map[string]any{
		"duration_s": d.Seconds(), "inserted": inserted, "rate_per_s": float64(inserted) / d.Seconds(),
		"size_half_bytes": half.SizeBytes, "size_end_bytes": last.SizeBytes, "size_drift": sizeDrift,
		"p99_half_ms": p99Half, "p99_end_ms": p99Last, "p99_drift": p99Drift,
		"rows_half": half.Rows, "rows_end": last.Rows,
	}
	pass := sizeDrift <= th.Float(t, "size_drift_max") && p99Drift <= th.Float(t, "p99_drift_max")
	writeResult(t, "FMN-12", measured, pass, map[string]any{"samples": samples})
	assert.LessOrEqual(t, sizeDrift, th.Float(t, "size_drift_max"), "table + index size stable")
	assert.LessOrEqual(t, p99Drift, th.Float(t, "p99_drift_max"), "p99 latency does not drift")
}
