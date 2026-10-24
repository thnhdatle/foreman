// SPDX-License-Identifier: Apache-2.0

// Command transactional-enqueue shows a job inserted in a transaction.
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

type InvoiceArgs struct {
	OrderID int64 `json:"order_id"`
}

func (InvoiceArgs) Kind() string { return "invoice" }

func main() {
	if err := run(context.Background()); err != nil {
		log.Println(err)
		os.Exit(1)
	}
}

func run(ctx context.Context) error {
	const schema = "example_tx"
	pool, err := exampledb.Open(ctx, schema)
	if err != nil {
		return err
	}
	defer pool.Close()
	if _, err := pool.Exec(ctx, `CREATE TABLE example_tx.orders (id bigint PRIMARY KEY)`); err != nil {
		return err
	}
	workers := foreman.NewWorkers()
	foreman.AddWorker(workers, foreman.WorkFunc[InvoiceArgs](func(_ context.Context, j *foreman.Job[InvoiceArgs]) error {
		fmt.Printf("invoicing order %d\n", j.Args.OrderID)
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

	committed, err := placeOrder(ctx, client, 1, true)
	if err != nil {
		return err
	}
	rolledBack, err := placeOrder(ctx, client, 2, false)
	if err != nil {
		return err
	}
	job, err := exampledb.WaitState(ctx, client, committed, foreman.StateCompleted, 10*time.Second)
	if err != nil {
		return err
	}
	fmt.Printf("committed order: job %d %s\n", job.ID, job.State)
	if _, err := client.JobGet(ctx, rolledBack); errors.Is(err, foreman.ErrNotFound) {
		fmt.Println("rolled back order: no job")
	}
	return nil
}

func placeOrder(ctx context.Context, c *foreman.Client, id int64, commit bool) (int64, error) {
	tx, err := c.Pool().Begin(ctx)
	if err != nil {
		return 0, err
	}
	defer func() { _ = tx.Rollback(ctx) }()
	if _, err := tx.Exec(ctx, `INSERT INTO example_tx.orders (id) VALUES ($1)`, id); err != nil {
		return 0, err
	}
	res, err := c.InsertTx(ctx, tx, InvoiceArgs{OrderID: id}, nil)
	if err != nil {
		return 0, err
	}
	if commit {
		return res.Job.ID, tx.Commit(ctx)
	}
	return res.Job.ID, tx.Rollback(ctx)
}
