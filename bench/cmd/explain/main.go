// SPDX-License-Identifier: Apache-2.0

// Command explain loads 1M and 10M jobs, records EXPLAIN (ANALYZE, BUFFERS)
// of the fetch queries and times fetch rounds, and prints the results as
// Markdown.
package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"os"
	"slices"
	"strings"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/thnhdatle/foreman/internal/adapter/pgstore"
	"github.com/thnhdatle/foreman/internal/domain/job"
	"github.com/thnhdatle/foreman/migrate"
)

func main() {
	const local = "postgres://foreman:foreman@localhost:55432/foreman?sslmode=disable" //nolint:gosec // G101: local dev db
	url := flag.String("database-url", local, "Postgres URL")
	sizes := flag.String("sizes", "1000000,10000000", "comma-separated row counts")
	flag.Parse()
	if err := run(context.Background(), *url, *sizes); err != nil {
		fmt.Fprintln(os.Stderr, "explain:", err)
		os.Exit(1)
	}
}

func run(ctx context.Context, url, sizes string) error {
	pool, err := pgxpool.New(ctx, url)
	if err != nil {
		return err
	}
	defer pool.Close()
	for _, s := range strings.Split(sizes, ",") {
		var n int
		if _, err := fmt.Sscan(s, &n); err != nil {
			return err
		}
		if err := measure(ctx, pool, n); err != nil {
			return err
		}
	}
	return nil
}

const schema = "explain_bench"

// load creates n jobs in 4 queues: 10% available (over 1,000 keys in queue
// q0), 2% running, the rest completed, like a busy queue with retention.
func load(ctx context.Context, pool *pgxpool.Pool, n int) error {
	if _, err := pool.Exec(ctx, `DROP SCHEMA IF EXISTS explain_bench CASCADE`); err != nil {
		return err
	}
	if err := migrate.Up(ctx, pool, migrate.WithSchema(schema)); err != nil {
		return err
	}
	_, err := pool.Exec(ctx, `INSERT INTO explain_bench.foreman_jobs
		(kind, queue, args, state, priority, scheduled_at, created_at, finalized_at, concurrency_key, attempted_by)
		SELECT 'k', 'q' || (g % 4), '{}',
		  (CASE WHEN g % 50 = 1 THEN 'running' WHEN g % 10 = 0 THEN 'available' ELSE 'completed' END)
		    ::explain_bench.foreman_state,
		  1 + (g / 7) % 4, now() + g * interval '1 ms', now(),
		  CASE WHEN g % 50 = 1 OR g % 10 = 0 THEN NULL ELSE now() END,
		  'tenant-' || (g / 4) % 1000, '{bench}'
		FROM generate_series(1, $1) g`, n)
	if err != nil {
		return err
	}
	_, err = pool.Exec(ctx, `VACUUM ANALYZE explain_bench.foreman_jobs`)
	return err
}

func measure(ctx context.Context, pool *pgxpool.Pool, n int) error {
	start := time.Now()
	if err := load(ctx, pool, n); err != nil {
		return fmt.Errorf("load %d: %w", n, err)
	}
	fmt.Printf("### %d jobs (loaded in %s)\n\n", n, time.Since(start).Round(time.Second))
	queries := []struct{ title, sql string }{
		{"Plain fetch of 50 jobs (queue q0)", `UPDATE explain_bench.foreman_jobs j
			SET state = 'running', attempt = j.attempt + 1
			FROM (SELECT id FROM explain_bench.foreman_jobs WHERE queue = 'q0' AND state = 'available'
			ORDER BY priority, scheduled_at, id LIMIT 50 FOR UPDATE SKIP LOCKED) p WHERE j.id = p.id`},
		{"Key enumeration (loose index scan, first 1,000 keys)", `WITH RECURSIVE keys AS (
			(SELECT concurrency_key AS key FROM explain_bench.foreman_jobs WHERE queue = 'q0' AND state = 'available'
			 AND concurrency_key IS NOT NULL ORDER BY concurrency_key, priority, scheduled_at, id LIMIT 1)
			UNION ALL SELECT n.key FROM keys k CROSS JOIN LATERAL (SELECT concurrency_key AS key
			FROM explain_bench.foreman_jobs WHERE queue = 'q0' AND state = 'available' AND concurrency_key > k.key
			ORDER BY concurrency_key, priority, scheduled_at, id LIMIT 1) n)
			SELECT count(*) FROM (SELECT * FROM keys LIMIT 1000) x`},
		{"Running count per key", `SELECT concurrency_key, count(*) FROM explain_bench.foreman_jobs
			WHERE queue = 'q0' AND state = 'running' GROUP BY concurrency_key`},
	}
	for _, q := range queries {
		plan, err := explain(ctx, pool, q.sql)
		if err != nil {
			return fmt.Errorf("%s: %w", q.title, err)
		}
		fmt.Printf("%s:\n\n```text\n%s\n```\n\n", q.title, plan)
	}
	return timeFetches(ctx, pool)
}

func explain(ctx context.Context, pool *pgxpool.Pool, sql string) (string, error) {
	var lines []string
	err := pgx.BeginFunc(ctx, pool, func(tx pgx.Tx) error {
		rows, err := tx.Query(ctx, "EXPLAIN (ANALYZE, BUFFERS, COSTS OFF) "+sql)
		if err != nil {
			return err
		}
		defer rows.Close()
		for rows.Next() {
			var l string
			if err := rows.Scan(&l); err != nil {
				return err
			}
			lines = append(lines, l)
		}
		if err := rows.Err(); err != nil {
			return err
		}
		return errRollback // EXPLAIN ANALYZE ran the UPDATE: leave the data unchanged
	})
	if err != nil && !errors.Is(err, errRollback) {
		return "", err
	}
	return strings.Join(lines, "\n"), nil
}

var errRollback = errors.New("rollback")

// timeFetches measures fetch rounds through the real store: 200 plain
// fetches of 50 jobs, and 200 keyed fetches with a per-key limit.
func timeFetches(ctx context.Context, pool *pgxpool.Pool) error {
	store, err := pgstore.New(pool, schema)
	if err != nil {
		return err
	}
	var plain, stats, keyed []time.Duration
	for i := 0; i < 200; i++ {
		t := time.Now()
		plainParams := job.FetchParams{Queue: "q1", ClientID: "bench", Now: time.Now(), Limit: 50}
		if _, err := store.Fetch(ctx, plainParams); err != nil {
			return err
		}
		plain = append(plain, time.Since(t))
		t = time.Now()
		ks, err := store.KeyStats(ctx, "q0", 1000, true)
		if err != nil {
			return err
		}
		stats = append(stats, time.Since(t))
		quotas := make([]job.KeyQuota, 0, 50)
		for _, k := range ks[:min(50, len(ks))] {
			quotas = append(quotas, job.KeyQuota{Key: k.Key, Quota: 1})
		}
		t = time.Now()
		if _, err := store.Fetch(ctx, job.FetchParams{
			Queue: "q0", ClientID: "bench", Now: time.Now(), Keyed: true, Quotas: quotas, PerKeyLimit: 5,
		}); err != nil {
			return err
		}
		keyed = append(keyed, time.Since(t))
	}
	fmt.Printf("Fetch round latency over 200 rounds (median / p99):\n\n")
	fmt.Printf("| Round | Median | p99 |\n|---|---|---|\n")
	for _, r := range []struct {
		name string
		ds   []time.Duration
	}{{"plain fetch of 50", plain}, {"key enumeration (1,000 keys)", stats}, {"keyed fetch, 50 keys, limit 5", keyed}} {
		slices.Sort(r.ds)
		fmt.Printf("| %s | %s | %s |\n", r.name, r.ds[len(r.ds)/2].Round(10*time.Microsecond),
			r.ds[len(r.ds)*99/100].Round(10*time.Microsecond))
	}
	fmt.Println()
	return nil
}
