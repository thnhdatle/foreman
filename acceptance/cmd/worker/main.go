// SPDX-License-Identifier: Apache-2.0

// Command worker is a Foreman client process used by the multi-process
// acceptance criteria (kill -9, SIGSTOP, concurrent unique inserts).
package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"log/slog"
	"net"
	"net/http"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"

	"github.com/thnhdatle/foreman"
	"github.com/thnhdatle/foreman/acceptance/internal/accjobs"
)

type options struct {
	databaseURL string
	schema      string
	id          string
	queues      string
	mode        string
	heartbeat   time.Duration
	rescueAfter time.Duration
	metricsAddr string
	record      bool
	inserts     int
	goroutines  int
	keys        int
	run         int64
}

func main() {
	var o options
	flag.StringVar(&o.databaseURL, "database-url", os.Getenv("FOREMAN_DATABASE_URL"), "Postgres URL")
	flag.StringVar(&o.schema, "schema", "public", "schema")
	flag.StringVar(&o.id, "id", "", "client id")
	flag.StringVar(&o.queues, "queues", "default:50", "comma-separated name:workers")
	flag.StringVar(&o.mode, "mode", "work", "work or insert")
	flag.DurationVar(&o.heartbeat, "heartbeat", 2*time.Second, "heartbeat interval")
	flag.DurationVar(&o.rescueAfter, "rescue-after", 10*time.Second, "rescue stuck jobs after")
	flag.StringVar(&o.metricsAddr, "metrics-addr", "", "serve /metrics on this address")
	flag.BoolVar(&o.record, "record", true, "record executions")
	flag.IntVar(&o.inserts, "inserts", 0, "insert mode: number of inserts")
	flag.IntVar(&o.goroutines, "goroutines", 1, "insert mode: concurrent inserters")
	flag.IntVar(&o.keys, "keys", 1, "insert mode: distinct unique keys")
	flag.Int64Var(&o.run, "run", 0, "insert mode: run id")
	flag.Parse()

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	err := run(ctx, o)
	stop()
	if err != nil {
		fmt.Fprintln(os.Stderr, "worker:", err)
		os.Exit(1)
	}
}

func run(ctx context.Context, o options) error {
	pcfg, err := pgxpool.ParseConfig(o.databaseURL)
	if err != nil {
		return fmt.Errorf("parse url: %w", err)
	}
	pcfg.MaxConns = 40
	pool, err := pgxpool.NewWithConfig(ctx, pcfg)
	if err != nil {
		return fmt.Errorf("pool: %w", err)
	}
	defer pool.Close()
	if o.mode == "insert" {
		return insertUnique(ctx, pool, o)
	}
	queues, err := parseQueues(o.queues)
	if err != nil {
		return err
	}
	reg := prometheus.NewRegistry()
	workers := foreman.NewWorkers()
	foreman.AddWorker(workers, &accjobs.NoopWorker{Pool: pool, Schema: o.schema, Record: o.record})
	foreman.AddWorker(workers, accjobs.UniqueWorker{})
	client, err := foreman.NewClient(pool, foreman.Config{
		Queues: queues, Workers: workers, Schema: o.schema, ID: o.id, HeartbeatInterval: o.heartbeat,
		RescueStuckJobsAfter: o.rescueAfter, Registerer: reg,
		Logger: slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelWarn})),
	})
	if err != nil {
		return fmt.Errorf("client: %w", err)
	}
	if o.metricsAddr != "" {
		ln, err := (&net.ListenConfig{}).Listen(ctx, "tcp", o.metricsAddr)
		if err != nil {
			return fmt.Errorf("metrics listen: %w", err)
		}
		srv := &http.Server{Handler: promhttp.HandlerFor(reg, promhttp.HandlerOpts{}), ReadHeaderTimeout: 5 * time.Second}
		go func() { _ = srv.Serve(ln) }()
		defer func() { _ = srv.Close() }()
	}
	if err := client.Start(ctx); err != nil {
		return fmt.Errorf("start: %w", err)
	}
	fmt.Printf("ready %s\n", client.ID())
	<-ctx.Done()
	stopCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), 30*time.Second)
	defer cancel()
	if err := client.StopAndCancel(stopCtx); err != nil && !errors.Is(err, context.DeadlineExceeded) {
		return fmt.Errorf("stop: %w", err)
	}
	return nil
}

func parseQueues(s string) (map[string]foreman.QueueConfig, error) {
	out := map[string]foreman.QueueConfig{}
	for _, part := range strings.Split(s, ",") {
		name, workers, ok := strings.Cut(strings.TrimSpace(part), ":")
		if !ok {
			return nil, fmt.Errorf("queue %q: want name:workers", part)
		}
		n, err := strconv.Atoi(workers)
		if err != nil {
			return nil, fmt.Errorf("queue %q: %w", part, err)
		}
		out[name] = foreman.QueueConfig{MaxWorkers: n, FetchInterval: 200 * time.Millisecond}
	}
	return out, nil
}

// insertUnique performs o.inserts unique inserts over o.keys keys from
// o.goroutines goroutines, half through foreman_insert_job (FMN-08).
func insertUnique(ctx context.Context, pool *pgxpool.Pool, o options) error {
	client, err := foreman.NewClient(pool, foreman.Config{Schema: o.schema})
	if err != nil {
		return fmt.Errorf("client: %w", err)
	}
	fmt.Println("ready insert")
	per := o.inserts / o.goroutines
	var wg sync.WaitGroup
	errs := make(chan error, o.goroutines)
	for g := 0; g < o.goroutines; g++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := 0; i < per; i++ {
				n := g*per + i
				big := accjobs.BigBase + int64(n%o.keys)
				if err := accjobs.InsertUnique(ctx, client, pool, o.schema, o.run, big, n%2 == 0); err != nil {
					errs <- err
					return
				}
			}
		}()
	}
	wg.Wait()
	close(errs)
	for err := range errs {
		return err
	}
	fmt.Println("done")
	return nil
}
