// SPDX-License-Identifier: Apache-2.0

//go:build integration

package foremantest_test

import (
	"context"
	"os"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/thnhdatle/foreman"
	"github.com/thnhdatle/foreman/foremantest"
	"github.com/thnhdatle/foreman/internal/testutil/pgtest"
)

var db *pgtest.Database

func TestMain(m *testing.M) { os.Exit(pgtest.Main(m, &db)) }

type fakeTB struct {
	testing.TB
	failed bool
}

func (f *fakeTB) Helper()               {}
func (f *fakeTB) Fatalf(string, ...any) { f.failed = true }

func TestRequireInsertedAndWorkInline(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	pool := db.Pool(t)
	schema := db.Schema(t, pool)
	workers := foreman.NewWorkers()
	foreman.AddWorker(workers, foreman.WorkFunc[mailArgs](func(context.Context, *foreman.Job[mailArgs]) error { return nil }))
	client, err := foreman.NewClient(pool, foreman.Config{Schema: schema, Workers: workers})
	require.NoError(t, err)

	_, err = client.Insert(ctx, mailArgs{To: "b@example.com"}, nil)
	require.NoError(t, err)
	row := foremantest.RequireInserted(ctx, t, client, mailArgs{To: "b@example.com"})
	assert.Equal(t, "mail", row.Kind)
	foremantest.RequireNotInserted(ctx, t, client, mailArgs{To: "nobody@example.com"})

	ftb := &fakeTB{TB: t}
	foremantest.RequireInserted(ctx, ftb, client, mailArgs{To: "nobody@example.com"})
	assert.True(t, ftb.failed, "missing job fails the test")
	ftb = &fakeTB{TB: t}
	foremantest.RequireNotInserted(ctx, ftb, client, mailArgs{To: "b@example.com"})
	assert.True(t, ftb.failed, "present job fails the test")

	done := foremantest.WorkInline(ctx, t, client, mailArgs{To: "c@example.com"}, &foreman.InsertOpts{Queue: "mail"})
	assert.Equal(t, foreman.StateCompleted, done.State)
	assert.Equal(t, "mail", done.Queue)
	assert.Equal(t, []string{client.ID()}, done.AttemptedBy)
}
