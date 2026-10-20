// SPDX-License-Identifier: Apache-2.0

// Command multi-tenant-fairness shows a small tenant served promptly despite
// a large tenant's backlog, with a per-tenant concurrency limit.
package main

import (
	"context"
	"errors"
	"fmt"
	"log"
	"os"
	"sync"
	"time"

	"github.com/thnhdatle/foreman"
	"github.com/thnhdatle/foreman/examples/internal/exampledb"
)

// ReportArgs belong to a tenant.
type ReportArgs struct {
	Tenant string `json:"tenant"`
	N      int    `json:"n"`
}

// Kind identifies the job type.
func (ReportArgs) Kind() string { return "report" }

func main() {
	if err := run(context.Background()); err != nil {
		log.Println(err)
		os.Exit(1)
	}
}

func run(ctx context.Context) error {
	const schema = "example_fairness"
	pool, err := exampledb.Open(ctx, schema)
	if err != nil {
		return err
	}
	defer pool.Close()
	var mu sync.Mutex
	done := make(chan struct{})
	const backlog, small = 200, 5
	positions := map[string][]int{}
	var count int
	workers := foreman.NewWorkers()
	foreman.AddWorker(workers, foreman.WorkFunc[ReportArgs](func(_ context.Context, j *foreman.Job[ReportArgs]) error {
		<-time.After(5 * time.Millisecond)
		mu.Lock()
		defer mu.Unlock()
		count++
		positions[j.Args.Tenant] = append(positions[j.Args.Tenant], count)
		if count == backlog+small {
			close(done)
		}
		return nil
	}))

	// Big tenant first: a backlog of 200 jobs, then 5 jobs of a small tenant.
	producer, err := foreman.NewClient(pool, foreman.Config{Schema: schema})
	if err != nil {
		return err
	}
	params := make([]foreman.InsertParams, 0, backlog+small)
	for i := 0; i < backlog; i++ {
		params = append(params, foreman.InsertParams{
			Args:       ReportArgs{Tenant: "big", N: i},
			InsertOpts: &foreman.InsertOpts{ConcurrencyKey: "big"},
		})
	}
	for i := 0; i < small; i++ {
		params = append(params, foreman.InsertParams{
			Args:       ReportArgs{Tenant: "small", N: i},
			InsertOpts: &foreman.InsertOpts{ConcurrencyKey: "small"},
		})
	}
	if _, err := producer.InsertMany(ctx, params); err != nil {
		return err
	}

	client, err := foreman.NewClient(pool, foreman.Config{
		Schema: schema, Workers: workers,
		Queues: map[string]foreman.QueueConfig{"default": {MaxWorkers: 4, PerKeyLimit: 3, Fairness: true}},
	})
	if err != nil {
		return err
	}
	if err := client.Start(ctx); err != nil {
		return err
	}
	defer func() { _ = client.Stop(ctx) }()
	select {
	case <-done:
	case <-time.After(60 * time.Second):
		return errors.New("jobs did not finish within 60s")
	}
	mu.Lock()
	defer mu.Unlock()
	last := positions["small"][len(positions["small"])-1]
	fmt.Printf("small tenant finished at position %d of %d\n", last, backlog+small)
	fmt.Printf("small tenant served before the backlog drained: %t\n", last < backlog)
	return nil
}
