// SPDX-License-Identifier: Apache-2.0

// Command cancel-long-job cancels a running job.
package main

import (
	"context"
	"fmt"
	"log"
	"os"
	"time"

	"github.com/thnhdatle/foreman"
	"github.com/thnhdatle/foreman/examples/internal/exampledb"
)

type ExportArgs struct {
	Rows int `json:"rows"`
}

func (ExportArgs) Kind() string { return "export" }

func main() {
	if err := run(context.Background()); err != nil {
		log.Println(err)
		os.Exit(1)
	}
}

func run(ctx context.Context) error {
	const schema = "example_cancel"
	pool, err := exampledb.Open(ctx, schema)
	if err != nil {
		return err
	}
	defer pool.Close()
	started := make(chan struct{}, 1)
	workers := foreman.NewWorkers()
	foreman.AddWorker(workers, foreman.WorkFunc[ExportArgs](func(ctx context.Context, j *foreman.Job[ExportArgs]) error {
		started <- struct{}{}
		for i := 0; i < j.Args.Rows; i++ {
			select {
			case <-ctx.Done():
				fmt.Printf("export stopped after %d rows: %v\n", i, context.Cause(ctx))
				return ctx.Err()
			case <-time.After(10 * time.Millisecond):
			}
			if i%100 == 0 {
				foreman.Heartbeat(ctx)
			}
		}
		return nil
	}))
	client, err := foreman.NewClient(pool, foreman.Config{
		Schema: schema, Workers: workers, Queues: map[string]foreman.QueueConfig{"default": {}},
	})
	if err != nil {
		return err
	}
	if err := client.Start(ctx); err != nil {
		return err
	}
	defer func() { _ = client.Stop(ctx) }()
	res, err := client.Insert(ctx, ExportArgs{Rows: 100_000}, nil)
	if err != nil {
		return err
	}
	<-started
	if _, err := client.JobCancel(ctx, res.Job.ID); err != nil {
		return err
	}
	job, err := exampledb.WaitState(ctx, client, res.Job.ID, foreman.StateCancelled, 10*time.Second)
	if err != nil {
		return err
	}
	fmt.Printf("job cancelled: state=%s\n", job.State)
	return nil
}
