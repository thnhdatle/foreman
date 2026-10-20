// SPDX-License-Identifier: Apache-2.0

package foreman_test

import (
	"context"
	"errors"
	"fmt"
	"log"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/thnhdatle/foreman"
	"github.com/thnhdatle/foreman/migrate"
)

type EmailArgs struct {
	UserID int64  `json:"user_id"`
	Tmpl   string `json:"tmpl"`
}

func (EmailArgs) Kind() string { return "email" }

type EmailWorker struct{}

func (w *EmailWorker) Work(ctx context.Context, job *foreman.Job[EmailArgs]) error {
	fmt.Printf("sending %s to user %d\n", job.Args.Tmpl, job.Args.UserID)
	return nil
}

// Example shows the whole lifecycle: migrate, register workers, start a
// client, enqueue in a transaction, stop.
func Example() {
	if err := run(context.Background()); err != nil {
		log.Fatal(err)
	}
}

func run(ctx context.Context) error {
	pool, err := pgxpool.New(ctx, "postgres://foreman:foreman@localhost:55432/foreman")
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

	tx, err := pool.Begin(ctx)
	if err != nil {
		return err
	}
	defer func() { _ = tx.Rollback(ctx) }()
	// ... insert the user in tx ...
	if _, err := client.InsertTx(ctx, tx, EmailArgs{UserID: 42, Tmpl: "welcome"},
		&foreman.InsertOpts{ConcurrencyKey: "tenant-7"}); err != nil {
		return err
	}
	return tx.Commit(ctx)
}

func ExampleAddWorker() {
	workers := foreman.NewWorkers()
	foreman.AddWorker(workers, foreman.WorkFunc[EmailArgs](func(ctx context.Context, job *foreman.Job[EmailArgs]) error {
		return nil
	}), foreman.WithTimeout(time.Minute), foreman.WithMaxAttempts(5),
		foreman.WithRetryPolicy(foreman.ConstantBackoff{Delay: 10 * time.Second}))
	fmt.Println(workers.Kinds())
	// Output: [email]
}

func ExampleNewWorkers() {
	workers := foreman.NewWorkers()
	foreman.AddWorker(workers, &EmailWorker{})
	fmt.Println(len(workers.Kinds()))
	// Output: 1
}

func ExampleNewClient() {
	var pool *pgxpool.Pool // your application's pool
	// A client without queues only inserts and administers jobs.
	_, err := foreman.NewClient(pool, foreman.Config{})
	fmt.Println(errors.Is(err, foreman.ErrInvalidArgument))
	// Output: true
}

func ExampleClient_Insert() {
	var client *foreman.Client
	res, err := client.Insert(context.Background(), EmailArgs{UserID: 1, Tmpl: "reset"}, &foreman.InsertOpts{
		Queue:      "mail",
		Priority:   1,
		UniqueOpts: &foreman.UniqueOpts{ByArgs: true, ByPeriod: time.Hour},
	})
	if err == nil && res.UniqueSkippedAsDuplicate {
		fmt.Println("already queued as job", res.Job.ID)
	}
}

func ExampleClient_InsertTx() {
	var (
		client *foreman.Client
		tx     pgx.Tx
	)
	// The job exists only if tx commits.
	_, _ = client.InsertTx(context.Background(), tx, EmailArgs{UserID: 2, Tmpl: "welcome"}, nil)
}

func ExampleClient_InsertMany() {
	var client *foreman.Client
	params := make([]foreman.InsertParams, 0, 100)
	for i := int64(0); i < 100; i++ {
		params = append(params, foreman.InsertParams{Args: EmailArgs{UserID: i, Tmpl: "digest"}})
	}
	_, _ = client.InsertMany(context.Background(), params) // COPY for batches of 10 or more
}

func ExampleClient_InsertManyTx() {
	var (
		client *foreman.Client
		tx     pgx.Tx
	)
	_, _ = client.InsertManyTx(context.Background(), tx, []foreman.InsertParams{
		{Args: EmailArgs{UserID: 1}}, {Args: EmailArgs{UserID: 2}},
	})
}

func ExampleClient_Start() {
	var client *foreman.Client
	ctx := context.Background()
	if err := client.Start(ctx); err != nil {
		log.Fatal(err)
	}
	// On shutdown: finish running jobs for up to 30s, then cancel them.
	stopCtx, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()
	_ = client.Stop(stopCtx)
}

func ExampleClient_StopAndCancel() {
	var client *foreman.Client
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	_ = client.StopAndCancel(ctx) // running jobs see their context cancelled
}

func ExampleClient_JobCancel() {
	var client *foreman.Client
	job, err := client.JobCancel(context.Background(), 123)
	if err == nil && job.State == foreman.StateRunning {
		fmt.Println("cancel requested; the job ends as cancelled when Work returns")
	}
}

func ExampleClient_JobRetry() {
	var client *foreman.Client
	// Move a dead-lettered job back to available with attempt 0.
	_, _ = client.JobRetry(context.Background(), 123)
}

func ExampleClient_JobDiscard() {
	var client *foreman.Client
	_, _ = client.JobDiscard(context.Background(), 123)
}

func ExampleClient_JobGet() {
	var client *foreman.Client
	job, err := client.JobGet(context.Background(), 123)
	if errors.Is(err, foreman.ErrNotFound) {
		return
	}
	fmt.Println(job.State, len(job.Errors))
}

func ExampleClient_JobList() {
	var client *foreman.Client
	params := &foreman.JobListParams{States: []foreman.State{foreman.StateDiscarded}, Limit: 100}
	for {
		page, err := client.JobList(context.Background(), params)
		if err != nil {
			return
		}
		for _, j := range page.Jobs {
			fmt.Println(j.ID, j.Kind)
		}
		if page.NextCursor == "" {
			return
		}
		params.Cursor = page.NextCursor
	}
}

func ExampleClient_JobRetryMany() {
	var client *foreman.Client
	n, _ := client.JobRetryMany(context.Background(), &foreman.JobFilter{
		Kinds: []string{"email"}, States: []foreman.State{foreman.StateDiscarded},
		CreatedAfter: time.Now().Add(-24 * time.Hour),
	})
	fmt.Println("retried", n)
}

func ExampleClient_JobDiscardMany() {
	var client *foreman.Client
	_, _ = client.JobDiscardMany(context.Background(), &foreman.JobFilter{Queues: []string{"legacy"}})
}

func ExampleClient_JobDeleteMany() {
	var client *foreman.Client
	_, _ = client.JobDeleteMany(context.Background(), &foreman.JobFilter{
		States: []foreman.State{foreman.StateCompleted}, CreatedBefore: time.Now().Add(-30 * 24 * time.Hour),
	})
}

func ExampleClient_JobWork() {
	var client *foreman.Client
	res, _ := client.Insert(context.Background(), EmailArgs{UserID: 3}, nil)
	job, _ := client.JobWork(context.Background(), res.Job.ID) // runs it now, synchronously
	fmt.Println(job.State)
}

func ExampleClient_QueuePause() {
	var client *foreman.Client
	ctx := context.Background()
	_ = client.QueuePause(ctx, "mail") // running jobs finish; nothing new is fetched
	_ = client.QueueResume(ctx, "mail")
}

func ExampleClient_QueueResume() {
	var client *foreman.Client
	_ = client.QueueResume(context.Background(), "mail")
}

func ExampleClient_QueueList() {
	var client *foreman.Client
	queues, _ := client.QueueList(context.Background())
	for _, q := range queues {
		fmt.Println(q.Name, q.Paused, q.Available, q.Running)
	}
}

func ExampleClient_LeaderGet() {
	var client *foreman.Client
	if l, err := client.LeaderGet(context.Background()); err == nil {
		fmt.Println(l.LeaderID, l.ExpiresAt)
	}
}

func ExampleClient_PeriodicList() {
	var client *foreman.Client
	ps, _ := client.PeriodicList(context.Background())
	for _, p := range ps {
		fmt.Println(p.Name, p.LastTick)
	}
}

func ExampleDecodeArgs() {
	row := &foreman.JobRow{ID: 1, Kind: "email", Args: []byte(`{"user_id":7,"tmpl":"welcome"}`)}
	args, err := foreman.DecodeArgs[EmailArgs](row)
	fmt.Println(args.UserID, args.Tmpl, err)
	// Output: 7 welcome <nil>
}

func ExampleClientFromContext() {
	_ = foreman.WorkFunc[EmailArgs](func(ctx context.Context, job *foreman.Job[EmailArgs]) error {
		client := foreman.ClientFromContext(ctx)
		// Enqueue a follow-up job from inside Work.
		_, err := client.Insert(ctx, EmailArgs{UserID: job.Args.UserID, Tmpl: "follow-up"}, nil)
		return err
	})
}

func ExampleCompleteTx() {
	_ = foreman.WorkFunc[EmailArgs](func(ctx context.Context, job *foreman.Job[EmailArgs]) error {
		tx, err := foreman.ClientFromContext(ctx).Pool().Begin(ctx)
		if err != nil {
			return err
		}
		defer func() { _ = tx.Rollback(ctx) }()
		// ... write the job's effects in tx ...
		if err := foreman.CompleteTx(ctx, tx, job); err != nil {
			return err
		}
		return tx.Commit(ctx) // effects and completion commit together
	})
}

func ExampleSnooze() {
	err := foreman.Snooze(30 * time.Second)
	fmt.Println(err)
	// Output: job snoozed for 30s
}

func ExampleDiscard() {
	err := foreman.Discard(errors.New("template does not exist"))
	fmt.Println(err)
	// Output: job discarded: template does not exist
}

func ExampleCancel() {
	err := foreman.Cancel(errors.New("user deleted"))
	fmt.Println(err)
	// Output: job cancelled: user deleted
}

func ExampleHeartbeat() {
	_ = foreman.WorkFunc[EmailArgs](func(ctx context.Context, job *foreman.Job[EmailArgs]) error {
		for i := 0; i < 10; i++ {
			// ... one long step ...
			foreman.Heartbeat(ctx) // keeps the rescuer away between automatic heartbeats
		}
		return nil
	})
}

func ExampleCron() {
	s, err := foreman.Cron("0 9 * * MON")
	if err != nil {
		log.Fatal(err)
	}
	fmt.Println(s.Next(time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)))
	// Output: 2026-01-05 09:00:00 +0000 UTC
}

func ExampleMustCron() {
	job := &foreman.PeriodicJob{
		Name:     "daily-report",
		Schedule: foreman.MustCron("@daily"),
		Args:     func() (foreman.JobArgs, *foreman.InsertOpts) { return EmailArgs{Tmpl: "report"}, nil },
		CatchUp:  time.Hour,
	}
	fmt.Println(job.Name, job.Schedule != nil, job.Args != nil, job.CatchUp)
	// Output: daily-report true true 1h0m0s
}

func ExampleEvery() {
	s := foreman.Every(15 * time.Minute)
	fmt.Println(s.Next(time.Date(2026, 1, 1, 10, 7, 0, 0, time.UTC)))
	// Output: 2026-01-01 10:15:00 +0000 UTC
}

func ExampleExponentialBackoff() {
	p := foreman.ExponentialBackoff{Base: time.Second, Cap: time.Hour}
	now := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	next := p.NextRetry(foreman.JobRow{Attempt: 3}, now)
	fmt.Println(next.Sub(now) < 4*time.Second)
	// Output: true
}
