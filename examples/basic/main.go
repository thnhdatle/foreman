// SPDX-License-Identifier: Apache-2.0

// Command basic registers a worker, enqueues a job in a transaction together
// with a business row, and waits for the job to complete.
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

// EmailArgs are the arguments of an email job.
type EmailArgs struct {
	UserID int64  `json:"user_id"`
	Tmpl   string `json:"tmpl"`
}

// Kind identifies the job type.
func (EmailArgs) Kind() string { return "email" }

// EmailWorker sends emails.
type EmailWorker struct {
	done chan int64
}

// Work sends the email (here: prints it).
func (w *EmailWorker) Work(_ context.Context, job *foreman.Job[EmailArgs]) error {
	fmt.Printf("sending %q email to user %d (attempt %d)\n", job.Args.Tmpl, job.Args.UserID, job.Attempt)
	w.done <- job.ID
	return nil
}

func main() {
	if err := run(context.Background()); err != nil {
		log.Println(err)
		os.Exit(1)
	}
}

func run(ctx context.Context) error {
	const schema = "example_basic"
	pool, err := exampledb.Open(ctx, schema)
	if err != nil {
		return err
	}
	defer pool.Close()
	const users = `CREATE TABLE example_basic.users (id bigserial PRIMARY KEY, email text NOT NULL)`
	if _, err := pool.Exec(ctx, users); err != nil {
		return fmt.Errorf("create users: %w", err)
	}

	worker := &EmailWorker{done: make(chan int64, 1)}
	workers := foreman.NewWorkers()
	foreman.AddWorker(workers, worker)
	client, err := foreman.NewClient(pool, foreman.Config{
		Schema:  schema,
		Queues:  map[string]foreman.QueueConfig{"default": {MaxWorkers: 50, PerKeyLimit: 5, Fairness: true}},
		Workers: workers,
	})
	if err != nil {
		return err
	}
	if err := client.Start(ctx); err != nil {
		return err
	}
	defer func() { _ = client.Stop(ctx) }()

	// The user row and the welcome email job commit together.
	tx, err := pool.Begin(ctx)
	if err != nil {
		return err
	}
	defer func() { _ = tx.Rollback(ctx) }()
	var userID int64
	if err := tx.QueryRow(ctx, `INSERT INTO example_basic.users (email) VALUES ('ada@example.com') RETURNING id`).
		Scan(&userID); err != nil {
		return err
	}
	res, err := client.InsertTx(ctx, tx, EmailArgs{UserID: userID, Tmpl: "welcome"},
		&foreman.InsertOpts{ConcurrencyKey: "tenant-1"})
	if err != nil {
		return err
	}
	if err := tx.Commit(ctx); err != nil {
		return err
	}
	fmt.Printf("enqueued job %d\n", res.Job.ID)

	select {
	case id := <-worker.done:
		job, err := waitFinal(ctx, client, id)
		if err != nil {
			return err
		}
		fmt.Printf("job completed: id=%d state=%s\n", job.ID, job.State)
		return nil
	case <-time.After(30 * time.Second):
		return errors.New("job did not run within 30s")
	}
}

// waitFinal polls until the job's result is written (results are batched).
func waitFinal(ctx context.Context, c *foreman.Client, id int64) (*foreman.JobRow, error) {
	deadline := time.Now().Add(10 * time.Second)
	for {
		job, err := c.JobGet(ctx, id)
		if err != nil {
			return nil, err
		}
		if job.State == foreman.StateCompleted || time.Now().After(deadline) {
			return job, nil
		}
		<-time.After(20 * time.Millisecond)
	}
}
