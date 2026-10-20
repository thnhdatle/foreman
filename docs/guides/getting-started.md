# Getting started

Add Foreman to a Go application that already uses Postgres through
[pgx](https://github.com/jackc/pgx).

## 1. Install

```sh
go get github.com/thnhdatle/foreman                             # the library
go install github.com/thnhdatle/foreman/cmd/foreman@latest      # the admin CLI (optional)
```

## 2. Create the schema

From Go, at startup:

```go
if err := migrate.Up(ctx, pool); err != nil { // package github.com/thnhdatle/foreman/migrate
	return err
}
```

Or from the command line:

```sh
export FOREMAN_DATABASE_URL=postgres://user:pass@host:5432/app
foreman migrate up             # apply migrations
foreman migrate sql up         # print the SQL for your own migration tool
```

Every instance can call `migrate.Up` at startup. Use `migrate.WithSchema("jobs")` or
`--schema jobs` for a schema other than `public`. `Start` fails with `ErrSchemaNotMigrated` on an
older schema.

## 3. Write the program

A job kind is a type with a `Kind()` method. Its JSON encoding is the payload.

```go
package main

import (
	"context"
	"log"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/thnhdatle/foreman"
	"github.com/thnhdatle/foreman/migrate"
)

type EmailArgs struct {
	UserID int64  `json:"user_id"`
	Tmpl   string `json:"tmpl"`
}

func (EmailArgs) Kind() string { return "email" } // same constant for the zero value

type EmailWorker struct{}

func (w *EmailWorker) Work(ctx context.Context, job *foreman.Job[EmailArgs]) error {
	log.Printf("sending %q to user %d (attempt %d)", job.Args.Tmpl, job.Args.UserID, job.Attempt)
	return nil // nil = completed, error = retry
}

func main() {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	if err := run(ctx); err != nil {
		log.Fatal(err)
	}
}

func run(ctx context.Context) error {
	pool, err := pgxpool.New(ctx, os.Getenv("FOREMAN_DATABASE_URL"))
	if err != nil {
		return err
	}
	defer pool.Close()
	if err := migrate.Up(ctx, pool); err != nil {
		return err
	}

	workers := foreman.NewWorkers()
	foreman.AddWorker(workers, &EmailWorker{})
	client, err := foreman.NewClient(pool, foreman.Config{
		Queues:  map[string]foreman.QueueConfig{foreman.DefaultQueue: {MaxWorkers: 20}},
		Workers: workers,
	})
	if err != nil {
		return err
	}
	// Cancelling the Start context stops the client hard, so do not pass the signal context.
	if err := client.Start(context.WithoutCancel(ctx)); err != nil {
		return err
	}

	res, err := client.Insert(ctx, EmailArgs{UserID: 42, Tmpl: "welcome"}, nil)
	if err != nil {
		return err
	}
	log.Printf("inserted job %d", res.Job.ID)

	<-ctx.Done() // wait for Ctrl-C or SIGTERM
	stopCtx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	return client.Stop(stopCtx) // finish running jobs, then shut down
}
```

Run it with `FOREMAN_DATABASE_URL` set, then press Ctrl-C.

- `AddWorker` registers one worker per kind. Registering a kind twice panics.
- A client with no `Queues` only inserts. It needs neither `Workers` nor `Start`.
- Every client that serves a queue must register a worker for every kind in that queue.
- `InsertTx` commits a job with your own writes. See
  [transactional enqueue](transactional-enqueue.md).

## Next steps

- [Workers](workers.md), [retries and dead letters](retries-and-dead-letters.md)
- [Periodic jobs](periodic-jobs.md), [unique jobs](unique-jobs.md),
  [concurrency and fairness](concurrency-and-fairness.md)
- [Operations](operations.md) before going to production
