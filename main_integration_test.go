// SPDX-License-Identifier: Apache-2.0

//go:build integration

package foreman

import (
	"context"
	"io"
	"log/slog"
	"os"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/stretchr/testify/require"

	"github.com/thnhdatle/foreman/internal/testutil/pgtest"
)

var testDB *pgtest.Database

func TestMain(m *testing.M) { os.Exit(pgtest.Main(m, &testDB)) }

type itEnv struct {
	pool   *pgxpool.Pool
	schema string
}

func newIT(t *testing.T) *itEnv {
	t.Helper()
	pool := testDB.Pool(t)
	return &itEnv{pool: pool, schema: testDB.Schema(t, pool)}
}

// client creates a client on the test schema; it is stopped at cleanup.
func (e *itEnv) client(t *testing.T, cfg Config) *Client {
	t.Helper()
	cfg.Schema = e.schema
	if cfg.Logger == nil {
		cfg.Logger = slog.New(slog.NewTextHandler(io.Discard, nil))
	}
	c, err := NewClient(e.pool, cfg)
	require.NoError(t, err)
	return c
}

func (e *itEnv) started(t *testing.T, cfg Config) *Client {
	t.Helper()
	c := e.client(t, cfg)
	require.NoError(t, c.Start(context.Background()))
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()
		_ = c.StopAndCancel(ctx)
	})
	return c
}

func fastQueue(workers int) QueueConfig {
	return QueueConfig{MaxWorkers: workers, FetchInterval: 50 * time.Millisecond}
}
