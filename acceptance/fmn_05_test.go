// SPDX-License-Identifier: Apache-2.0

//go:build acceptance

package acceptance

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/stretchr/testify/require"

	"github.com/thnhdatle/foreman"
)

// FMN-05: pickup latency (created_at to attempted_at, both client clock) at a
// steady 1,000 single-job inserts per second.
func BenchmarkAcceptance_FMN05_PickupLatency(b *testing.B) {
	th := loadThresholds(b, "FMN-05")
	rate, clients, workers := th.Int(b, "insert_rate_per_s"), th.Int(b, "clients"), th.Int(b, "workers_per_client")
	d := time.Duration(th.Int(b, "duration_s")) * time.Second
	warmup := time.Duration(th.Int(b, "warmup_s")) * time.Second
	const schema = "acc_fmn05"
	pool := newPool(b, dbURL(), int32(clients*20+40))
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		freshSchema(b, pool, schema)
		perfClients(b, pool, schema, clients, foreman.QueueConfig{MaxWorkers: workers},
			func(context.Context, *foreman.Job[noopArgs]) error { return nil })
		producer, err := foreman.NewClient(pool, foreman.Config{Schema: schema, Logger: testLogger(b)})
		require.NoError(b, err)
		measureFrom := time.Now().Add(warmup)
		inserted, err := steadyInserts(producer, rate, warmup+d)
		require.NoError(b, err)
		require.True(b, waitFor(time.Minute, 100*time.Millisecond, func() bool {
			return countWhere(b, pool, schema, "state = 'completed'") == inserted
		}), "all jobs ran")
		p50, p99, n := pickupPercentiles(b, pool, schema, measureFrom)
		b.ReportMetric(p99, "p99_ms")
		measured := map[string]any{
			"inserted": inserted, "measured_jobs": n, "rate_per_s": float64(inserted) / (warmup + d).Seconds(),
			"p50_ms": p50, "p99_ms": p99, "duration_s": d.Seconds(),
		}
		pass := p99 < th.Float(b, "p99_ms_max")
		writeResult(b, "FMN-05", measured, pass, nil)
		if !pass {
			b.Fatalf("p99 pickup latency %.1f ms, threshold %v ms", p99, th.Float(b, "p99_ms_max"))
		}
	}
}

// steadyInserts inserts rate jobs per second, one statement per job, from
// 10 goroutines paced by tickers (open model: inserts do not wait for workers).
func steadyInserts(c *foreman.Client, rate int, d time.Duration) (int64, error) {
	const goroutines = 10
	interval := time.Duration(int64(time.Second) * goroutines / int64(rate))
	end := time.Now().Add(d)
	var n atomic.Int64
	var wg sync.WaitGroup
	errs := make(chan error, goroutines)
	for g := 0; g < goroutines; g++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			tick := time.NewTicker(interval)
			defer tick.Stop()
			for time.Now().Before(end) {
				<-tick.C
				if _, err := c.Insert(context.Background(), noopArgs{N: int(n.Add(1))}, nil); err != nil {
					errs <- err
					return
				}
			}
		}()
	}
	wg.Wait()
	close(errs)
	for err := range errs {
		return n.Load(), err
	}
	return n.Load(), nil
}

func pickupPercentiles(tb testing.TB, pool *pgxpool.Pool, schema string, from time.Time) (p50, p99 float64, n int64) {
	tb.Helper()
	q := fmt.Sprintf(`SELECT
		percentile_cont(0.5) WITHIN GROUP (ORDER BY extract(epoch FROM attempted_at - created_at)) * 1000,
		percentile_cont(0.99) WITHIN GROUP (ORDER BY extract(epoch FROM attempted_at - created_at)) * 1000,
		count(*)
		FROM %q.foreman_jobs WHERE created_at >= $1 AND attempted_at IS NOT NULL`, schema)
	require.NoError(tb, pool.QueryRow(context.Background(), q, from).Scan(&p50, &p99, &n))
	return p50, p99, n
}
