// SPDX-License-Identifier: Apache-2.0

package pgtest

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/modules/postgres"
	"go.uber.org/goleak"

	"github.com/thnhdatle/foreman/migrate"
)

// DefaultImage is the Postgres image used when FOREMAN_TEST_PG_IMAGE is unset.
const DefaultImage = "postgres:17"

// Database is a Postgres server shared by the tests of one package.
type Database struct {
	URL       string
	container testcontainers.Container
}

// Start returns the server named by FOREMAN_TEST_DATABASE_URL, or starts a
// container from FOREMAN_TEST_PG_IMAGE (default postgres:17).
func Start(ctx context.Context) (*Database, error) {
	if url := os.Getenv("FOREMAN_TEST_DATABASE_URL"); url != "" {
		return &Database{URL: url}, nil
	}
	image := os.Getenv("FOREMAN_TEST_PG_IMAGE")
	if image == "" {
		image = DefaultImage
	}
	ctr, err := postgres.Run(ctx, image,
		postgres.WithDatabase("foreman"),
		postgres.WithUsername("foreman"),
		postgres.WithPassword("foreman"),
		postgres.BasicWaitStrategies(),
		testcontainers.WithCmdArgs("-c", "max_connections=300", "-c", "fsync=off", "-c", "synchronous_commit=off"),
	)
	if err != nil {
		return nil, fmt.Errorf("pgtest: start %s: %w", image, err)
	}
	url, err := ctr.ConnectionString(ctx, "sslmode=disable")
	if err != nil {
		_ = testcontainers.TerminateContainer(ctr)
		return nil, fmt.Errorf("pgtest: connection string: %w", err)
	}
	return &Database{URL: url, container: ctr}, nil
}

// Close stops the container if this package started one.
func (d *Database) Close() {
	if d.container != nil {
		_ = testcontainers.TerminateContainer(d.container)
	}
}

// Main is a TestMain body for integration test packages: it starts the
// database, runs the tests and checks for goroutine leaks.
func Main(m *testing.M, db **Database) int {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	d, err := Start(ctx)
	cancel()
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		return 1
	}
	*db = d
	code := m.Run()
	d.Close()
	if code == 0 {
		if err := goleak.Find(LeakOptions()...); err != nil {
			fmt.Fprintln(os.Stderr, "goleak:", err)
			return 1
		}
	}
	return code
}

// LeakOptions ignores the background goroutines of testcontainers and pgx pools.
func LeakOptions() []goleak.Option {
	return []goleak.Option{
		goleak.IgnoreAnyFunction("github.com/testcontainers/testcontainers-go.(*Reaper).connect.func1"),
		goleak.IgnoreAnyFunction("github.com/testcontainers/testcontainers-go.(*reaperSpawner).connect.func1"),
		goleak.IgnoreAnyFunction("internal/poll.runtime_pollWait"),
		goleak.IgnoreAnyFunction("net/http.(*persistConn).writeLoop"),
		goleak.IgnoreAnyFunction("net/http.(*persistConn).readLoop"),
		goleak.IgnoreTopFunction("github.com/testcontainers/testcontainers-go.(*Reaper).connect.func1"),
	}
}

// Pool opens a pool on the database and closes it when the test ends.
func (d *Database) Pool(tb testing.TB) *pgxpool.Pool {
	tb.Helper()
	cfg, err := pgxpool.ParseConfig(d.URL)
	if err != nil {
		tb.Fatalf("pgtest: parse url: %v", err)
	}
	cfg.MaxConns = 20
	pool, err := pgxpool.NewWithConfig(context.Background(), cfg)
	if err != nil {
		tb.Fatalf("pgtest: pool: %v", err)
	}
	tb.Cleanup(pool.Close)
	return pool
}

// Schema creates a fresh schema with the Foreman migrations applied and
// drops it when the test ends. Each test gets its own schema, so tests run
// in parallel against one server without interfering (channels are
// schema-prefixed too).
func (d *Database) Schema(tb testing.TB, pool *pgxpool.Pool) string {
	tb.Helper()
	b := make([]byte, 6)
	_, _ = rand.Read(b)
	schema := "t_" + hex.EncodeToString(b)
	ctx := context.Background()
	if err := migrate.Up(ctx, pool, migrate.WithSchema(schema)); err != nil {
		tb.Fatalf("pgtest: migrate %s: %v", schema, err)
	}
	tb.Cleanup(func() {
		_, _ = pool.Exec(context.Background(), fmt.Sprintf(`DROP SCHEMA IF EXISTS %q CASCADE`, schema))
	})
	return schema
}
