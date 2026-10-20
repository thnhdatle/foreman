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

// noopArgs is the payload of throughput and latency jobs; Key spreads jobs
// over concurrency keys when the scenario needs them.
type noopArgs struct {
	N int `json:"n"`
}

func (noopArgs) Kind() string { return "perf_noop" }

// perfClients starts n clients serving queue "default" with the given queue
// config and a worker that runs fn; they are stopped at the end of the test.
func perfClients(tb testing.TB, pool *pgxpool.Pool, schema string, n int, q foreman.QueueConfig,
	fn func(context.Context, *foreman.Job[noopArgs]) error,
) []*foreman.Client {
	tb.Helper()
	clients := make([]*foreman.Client, n)
	for i := range clients {
		workers := foreman.NewWorkers()
		foreman.AddWorker(workers, foreman.WorkFunc[noopArgs](fn))
		c, err := foreman.NewClient(pool, foreman.Config{
			Schema: schema, Workers: workers, Logger: testLogger(tb), ID: fmt.Sprintf("perf-%d", i),
			Queues: map[string]foreman.QueueConfig{"default": q},
		})
		require.NoError(tb, err)
		require.NoError(tb, c.Start(context.Background()))
		clients[i] = c
	}
	tb.Cleanup(func() { stopAll(clients) })
	return clients
}

func stopAll(clients []*foreman.Client) {
	var wg sync.WaitGroup
	for _, c := range clients {
		wg.Add(1)
		go func() {
			defer wg.Done()
			ctx, cancel := context.WithTimeout(context.Background(), time.Minute)
			defer cancel()
			_ = c.StopAndCancel(ctx)
		}()
	}
	wg.Wait()
}

// prefill inserts n jobs through COPY in batches; keyOf gives each job's
// concurrency key ("" for none).
func prefill(tb testing.TB, c *foreman.Client, start, n int, keyOf func(i int) string) {
	tb.Helper()
	require.NoError(tb, insertRange(c, start, n, keyOf))
}

// insertRange is prefill for any goroutine: it returns the first error.
func insertRange(c *foreman.Client, start, n int, keyOf func(i int) string) error {
	const batch = 10_000
	params := make([]foreman.InsertParams, 0, batch)
	for i := start; i < start+n; i++ {
		var opts *foreman.InsertOpts
		if k := keyOf(i); k != "" {
			opts = &foreman.InsertOpts{ConcurrencyKey: k}
		}
		params = append(params, foreman.InsertParams{Args: noopArgs{N: i}, InsertOpts: opts})
		if len(params) == batch || i == start+n-1 {
			if _, err := c.InsertMany(context.Background(), params); err != nil {
				return err //nolint:wrapcheck // test helper
			}
			params = params[:0]
		}
	}
	return nil
}

// throughputRun measures completed jobs per second over d while a top-up
// goroutine keeps at least `floor` jobs available.
type throughputRun struct {
	jobsPerSec float64
	completed  int64
	topUps     int
}

func runThroughput(tb testing.TB, pool *pgxpool.Pool, schema string, clients, workers int, q foreman.QueueConfig,
	prefillN int, d time.Duration, keyOf func(i int) string,
) throughputRun {
	tb.Helper()
	ctx := context.Background()
	freshSchema(tb, pool, schema)
	producer, err := foreman.NewClient(pool, foreman.Config{Schema: schema, Logger: testLogger(tb)})
	require.NoError(tb, err)
	prefill(tb, producer, 0, prefillN, keyOf)
	_, err = pool.Exec(ctx, fmt.Sprintf(`VACUUM ANALYZE %q.foreman_jobs`, schema))
	require.NoError(tb, err)

	var done atomic.Int64
	q.MaxWorkers = workers
	perfClients(tb, pool, schema, clients, q, func(context.Context, *foreman.Job[noopArgs]) error {
		done.Add(1)
		return nil
	})
	// Keep the queue from draining: the run must be sustained for d.
	stop := make(chan struct{})
	var topUps int
	var topErr error
	var topWG sync.WaitGroup
	topWG.Add(1)
	go func() {
		defer topWG.Done()
		next := prefillN
		floor := int64(prefillN / 2)
		q := fmt.Sprintf(`SELECT count(*) FROM %q.foreman_jobs WHERE state = 'available'`, schema)
		for {
			select {
			case <-stop:
				return
			case <-time.After(2 * time.Second):
			}
			var available int64
			if topErr = pool.QueryRow(ctx, q).Scan(&available); topErr != nil {
				return
			}
			if available < floor {
				if topErr = insertRange(producer, next, prefillN/4, keyOf); topErr != nil {
					return
				}
				next += prefillN / 4
				topUps++
			}
		}
	}()
	<-time.After(15 * time.Second) // warm-up
	before, start := done.Load(), time.Now()
	<-time.After(d)
	completed := done.Load() - before
	elapsed := time.Since(start)
	close(stop)
	topWG.Wait()
	require.NoError(tb, topErr, "topping up the queue")
	return throughputRun{jobsPerSec: float64(completed) / elapsed.Seconds(), completed: completed, topUps: topUps}
}
