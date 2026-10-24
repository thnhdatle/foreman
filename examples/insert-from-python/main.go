// SPDX-License-Identifier: Apache-2.0

// Command insert-from-python runs a Go worker for jobs enqueued by a Python script.
package main

import (
	"context"
	"errors"
	"fmt"
	"log"
	"os"
	"os/exec"
	"time"

	"github.com/thnhdatle/foreman"
	"github.com/thnhdatle/foreman/examples/internal/exampledb"
)

type EmailArgs struct {
	To       string `json:"to"`
	Template string `json:"template"`
}

func (EmailArgs) Kind() string { return "email" }

func main() {
	if err := run(context.Background()); err != nil {
		log.Println(err)
		os.Exit(1)
	}
}

func run(ctx context.Context) error {
	const schema = "example_python"
	pool, err := exampledb.Open(ctx, schema)
	if err != nil {
		return err
	}
	defer pool.Close()
	done := make(chan int64, 1)
	workers := foreman.NewWorkers()
	foreman.AddWorker(workers, foreman.WorkFunc[EmailArgs](func(_ context.Context, j *foreman.Job[EmailArgs]) error {
		fmt.Printf("go worker sending %q to %s\n", j.Args.Template, j.Args.To)
		done <- j.ID
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

	cmd := exec.CommandContext(ctx, "uv", "run", "--quiet", "--no-project", "--python", "3.12",
		"--with", "psycopg[binary]==3.2.10", "python", "examples/insert-from-python/insert.py", schema)
	cmd.Stdout, cmd.Stderr = os.Stdout, os.Stderr
	if err := cmd.Run(); err != nil {
		return fmt.Errorf("python insert: %w", err)
	}
	select {
	case id := <-done:
		job, err := exampledb.WaitState(ctx, client, id, foreman.StateCompleted, 10*time.Second)
		if err != nil {
			return err
		}
		fmt.Printf("job %d %s\n", job.ID, job.State)
		return nil
	case <-time.After(30 * time.Second):
		return errors.New("job did not run within 30s")
	}
}
