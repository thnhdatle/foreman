// SPDX-License-Identifier: Apache-2.0

//go:build integration

package pgstore_test

import (
	"context"
	"encoding/json"
	"os"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/stretchr/testify/require"

	"github.com/thnhdatle/foreman/internal/adapter/pgstore"
	"github.com/thnhdatle/foreman/internal/domain/job"
	"github.com/thnhdatle/foreman/internal/domain/unique"
	"github.com/thnhdatle/foreman/internal/testutil/pgtest"
)

var db *pgtest.Database

func TestMain(m *testing.M) { os.Exit(pgtest.Main(m, &db)) }

var t0 = time.Date(2026, 6, 1, 12, 0, 0, 0, time.UTC)

type env struct {
	pool   *pgxpool.Pool
	store  *pgstore.Store
	schema string
}

func newEnv(t *testing.T) *env {
	t.Helper()
	pool := db.Pool(t)
	schema := db.Schema(t, pool)
	s, err := pgstore.New(pool, schema)
	require.NoError(t, err)
	return &env{pool: pool, store: s, schema: schema}
}

func params(kind, queue string) job.InsertParams {
	return job.InsertParams{
		Kind: kind, Queue: queue, Args: []byte(`{"n":1}`), State: job.StateAvailable, Priority: 2,
		MaxAttempts: 3, ScheduledAt: t0, CreatedAt: t0,
	}
}

func uniqueParams(kind string, args string, opts unique.Options) job.InsertParams {
	p := params(kind, "default")
	p.Args = []byte(args)
	id, err := unique.Identity(kind, p.Queue, p.Args, opts, t0)
	if err != nil {
		panic(err)
	}
	p.UniqueIdentity = id
	p.UniqueStates = unique.Mask(opts.States())
	return p
}

func (e *env) insert(t *testing.T, ps ...job.InsertParams) []job.InsertResult {
	t.Helper()
	res, err := e.store.Insert(context.Background(), e.pool, ps)
	require.NoError(t, err)
	return res
}

func (e *env) count(t *testing.T, where string, args ...any) int {
	t.Helper()
	var n int
	require.NoError(t, e.pool.QueryRow(context.Background(),
		`SELECT count(*) FROM "`+e.schema+`".foreman_jobs WHERE `+where, args...).Scan(&n))
	return n
}

func (e *env) exec(t *testing.T, sql string, args ...any) {
	t.Helper()
	_, err := e.pool.Exec(context.Background(), sql, args...)
	require.NoError(t, err)
}

func jsonUnmarshal(s string, v any) error { return json.Unmarshal([]byte(s), v) }

func pgstoreNames(schema string) (pgstore.Names, error) { return pgstore.NewNames(schema) }
