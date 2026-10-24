// SPDX-License-Identifier: Apache-2.0

// Command periodic enqueues a job every second through the leader.
package main

import (
	"context"
	"errors"
	"fmt"
	"log"
	"os"
	"time"

	"github.com/thnhdatle/foreman"
	"github.com/thnhdatle/foreman/examples/internal/exampledb"
)

type CleanupArgs struct{}

func (CleanupArgs) Kind() string { return "cleanup" }

func main() {
	if err := run(context.Background()); err != nil {
		log.Println(err)
		os.Exit(1)
	}
}

func run(ctx context.Context) error {
	const schema = "example_periodic"
	pool, err := exampledb.Open(ctx, schema)
	if err != nil {
		return err
	}
	defer pool.Close()
	ticks := make(chan struct{}, 10)
	workers := foreman.NewWorkers()
	foreman.AddWorker(workers, foreman.WorkFunc[CleanupArgs](func(context.Context, *foreman.Job[CleanupArgs]) error {
		ticks <- struct{}{}
		return nil
	}))
	client, err := foreman.NewClient(pool, foreman.Config{
		Schema: schema, Workers: workers, Queues: map[string]foreman.QueueConfig{"default": {}},
		PeriodicJobs: []*foreman.PeriodicJob{{
			Name:     "cleanup",
			Schedule: foreman.Every(time.Second),
			Args:     func() (foreman.JobArgs, *foreman.InsertOpts) { return CleanupArgs{}, nil },
			CatchUp:  time.Minute,
		}},
	})
	if err != nil {
		return err
	}
	if err := client.Start(ctx); err != nil {
		return err
	}
	defer func() { _ = client.Stop(ctx) }()
	for i := 1; i <= 3; i++ {
		select {
		case <-ticks:
			fmt.Printf("periodic job ran (%d)\n", i)
		case <-time.After(30 * time.Second):
			return errors.New("no tick within 30s")
		}
	}
	return nil
}
