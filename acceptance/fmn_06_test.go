// SPDX-License-Identifier: Apache-2.0

//go:build acceptance

package acceptance

import (
	"context"
	"fmt"
	"math"
	"math/rand/v2"
	"sync"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/thnhdatle/foreman"
)

type tenantArgs struct {
	Tenant string `json:"tenant"`
}

func (tenantArgs) Kind() string { return "perf_tenant" }

type fairnessScenario struct {
	fair     bool
	withA    bool
	clients  int
	workers  int
	jobTime  time.Duration
	backlog  int
	duration time.Duration
	rateA    float64
	rateB    float64
}

// runFairness runs one FMN-06 mode and returns B's waits (attempted_at -
// created_at) in milliseconds.
func runFairness(t *testing.T, pool *pgxpool.Pool, s fairnessScenario) []float64 {
	t.Helper()
	ctx := context.Background()
	const schema = "acc_fmn06"
	freshSchema(t, pool, schema)
	producer, err := foreman.NewClient(pool, foreman.Config{Schema: schema, Logger: testLogger(t)})
	require.NoError(t, err)
	if s.withA {
		params := make([]foreman.InsertParams, 0, 10_000)
		for i := 0; i < s.backlog; i++ {
			params = append(params, foreman.InsertParams{
				Args:       tenantArgs{Tenant: "A"},
				InsertOpts: &foreman.InsertOpts{ConcurrencyKey: "A"},
			})
			if len(params) == cap(params) || i == s.backlog-1 {
				_, err := producer.InsertMany(ctx, params)
				require.NoError(t, err)
				params = params[:0]
			}
		}
		_, err = pool.Exec(ctx, `VACUUM ANALYZE "acc_fmn06".foreman_jobs`)
		require.NoError(t, err)
	}
	clients := make([]*foreman.Client, s.clients)
	for i := range clients {
		workers := foreman.NewWorkers()
		foreman.AddWorker(workers, foreman.WorkFunc[tenantArgs](func(ctx context.Context, _ *foreman.Job[tenantArgs]) error {
			select {
			case <-ctx.Done():
				return ctx.Err()
			case <-time.After(s.jobTime):
				return nil
			}
		}))
		c, err := foreman.NewClient(pool, foreman.Config{
			Schema: schema, Workers: workers, Logger: testLogger(t), ID: fmt.Sprintf("fair-%d", i),
			Queues: map[string]foreman.QueueConfig{"default": {MaxWorkers: s.workers, Fairness: s.fair}},
		})
		require.NoError(t, err)
		require.NoError(t, c.Start(ctx))
		clients[i] = c
	}
	defer stopAll(clients)

	// Poisson arrivals: per 10 ms slot, a Poisson-distributed number of jobs.
	var wg sync.WaitGroup
	errs := make(chan error, 2)
	arrive := func(tenant string, rate float64, seed uint64) {
		defer wg.Done()
		rng := rand.New(rand.NewPCG(seed, seed))
		const slot = 10 * time.Millisecond
		tick := time.NewTicker(slot)
		defer tick.Stop()
		end := time.Now().Add(s.duration)
		for time.Now().Before(end) {
			<-tick.C
			n := poisson(rng, rate*slot.Seconds())
			if n == 0 {
				continue
			}
			params := make([]foreman.InsertParams, n)
			for i := range params {
				params[i] = foreman.InsertParams{
					Args:       tenantArgs{Tenant: tenant},
					InsertOpts: &foreman.InsertOpts{ConcurrencyKey: tenant},
				}
			}
			if _, err := producer.InsertMany(ctx, params); err != nil {
				errs <- err
				return
			}
		}
	}
	if s.withA {
		wg.Add(1)
		go arrive("A", s.rateA, 61)
	}
	wg.Add(1)
	go arrive("B", s.rateB, 62)
	wg.Wait()
	close(errs)
	for err := range errs {
		require.NoError(t, err)
	}
	// Every B job must have started before its wait is known.
	require.True(t, waitFor(20*time.Minute, time.Second, func() bool {
		return countWhere(t, pool, schema, "args->>'tenant' = 'B' AND attempted_at IS NULL") == 0
	}), "all B jobs started")
	rows, err := pool.Query(ctx, `SELECT extract(epoch FROM attempted_at - created_at) * 1000
		FROM "acc_fmn06".foreman_jobs WHERE args->>'tenant' = 'B'`)
	require.NoError(t, err)
	defer rows.Close()
	var waits []float64
	for rows.Next() {
		var w float64
		require.NoError(t, rows.Scan(&w))
		waits = append(waits, w)
	}
	require.NoError(t, rows.Err())
	return waits
}

// poisson draws from a Poisson distribution with mean lambda (Knuth).
func poisson(rng *rand.Rand, lambda float64) int {
	l, k, p := math.Exp(-lambda), 0, 1.0
	for {
		p *= rng.Float64()
		if p <= l {
			return k
		}
		k++
	}
}

func p95ms(ws []float64) float64 {
	ds := make([]time.Duration, len(ws))
	for i, w := range ws {
		ds[i] = time.Duration(w * float64(time.Millisecond))
	}
	return ms(percentile(ds, 95))
}

// FMN-06: with a standing backlog of tenant A, tenant B's p95 wait with
// fairness stays close to B alone and far below B's wait without fairness.
func TestAcceptance_FMN06_Fairness(t *testing.T) {
	th := loadThresholds(t, "FMN-06")
	jobTime := time.Duration(th.Int(t, "job_ms")) * time.Millisecond
	clients, workers := th.Int(t, "clients"), th.Int(t, "workers_per_client")
	capacity := float64(clients*workers) / jobTime.Seconds() // C, jobs per second
	base := fairnessScenario{
		clients: clients, workers: workers, jobTime: jobTime,
		backlog:  int(capacity * th.Float(t, "backlog_capacity_seconds")),
		duration: time.Duration(th.Int(t, "duration_s")) * time.Second,
		rateA:    th.Float(t, "a_rate_factor") * capacity, rateB: th.Float(t, "b_rate_factor") * capacity,
	}
	pool := newPool(t, dbURL(), int32(clients*20+40))

	fair := base
	fair.fair, fair.withA = true, true
	unfair := base
	unfair.withA = true
	alone := base
	alone.fair = true

	fairP95 := p95ms(runFairness(t, pool, fair))
	unfairP95 := p95ms(runFairness(t, pool, unfair))
	aloneP95 := p95ms(runFairness(t, pool, alone))

	limitAlone := math.Max(th.Float(t, "fair_vs_alone_factor_max")*aloneP95, th.Float(t, "fair_floor_ms"))
	limitUnfair := th.Float(t, "fair_vs_unfair_factor_max") * unfairP95
	measured := map[string]any{
		"capacity_jobs_per_s": capacity, "backlog_jobs": base.backlog, "b_p95_fair_ms": fairP95,
		"b_p95_unfair_ms": unfairP95, "b_p95_alone_ms": aloneP95, "limit_vs_alone_ms": limitAlone,
		"limit_vs_unfair_ms": limitUnfair,
	}
	pass := fairP95 <= limitAlone && fairP95 <= limitUnfair
	writeResult(t, "FMN-06", measured, pass, nil)
	assert.LessOrEqual(t, fairP95, limitAlone, "fair p95 vs max(2 x alone p95, floor)")
	assert.LessOrEqual(t, fairP95, limitUnfair, "fair p95 vs 0.2 x unfair p95")
}
