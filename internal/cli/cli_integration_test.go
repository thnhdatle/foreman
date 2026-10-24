// SPDX-License-Identifier: Apache-2.0

//go:build integration

package cli_test

import (
	"context"
	"errors"
	"flag"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/thnhdatle/foreman"
	"github.com/thnhdatle/foreman/foremantest"
	"github.com/thnhdatle/foreman/internal/cli"
	"github.com/thnhdatle/foreman/internal/testutil/pgtest"
)

var (
	db     *pgtest.Database
	update = flag.Bool("update", false, "rewrite golden files")
)

func TestMain(m *testing.M) {
	flag.Parse()
	time.Local = time.UTC
	os.Exit(pgtest.Main(m, &db))
}

type reportArgs struct {
	Name string `json:"name"`
}

func (reportArgs) Kind() string { return "report" }

func seed(t *testing.T) map[string]string {
	t.Helper()
	ctx := context.Background()
	pool := db.Pool(t)
	schema := db.Schema(t, pool)
	clk := foremantest.NewClock(time.Date(2026, 10, 1, 12, 0, 0, 0, time.UTC))
	workers := foreman.NewWorkers()
	foreman.AddWorker(workers, foreman.WorkFunc[reportArgs](func(_ context.Context, j *foreman.Job[reportArgs]) error {
		if j.Args.Name == "broken" {
			return foreman.Discard(errors.New("template missing"))
		}
		return nil
	}))
	c, err := foreman.NewClient(pool, foreman.Config{Schema: schema, Clock: clk, ID: "worker-1", Workers: workers})
	require.NoError(t, err)
	for _, a := range []struct {
		name  string
		queue string
		opts  foreman.InsertOpts
	}{
		{"daily", "reports", foreman.InsertOpts{Tags: []string{"finance"}}},
		{"weekly", "reports", foreman.InsertOpts{Priority: 1, ConcurrencyKey: "tenant-a"}},
		{"later", "reports", foreman.InsertOpts{ScheduledAt: clk.Now().Add(time.Hour)}},
		{"mail", "mail", foreman.InsertOpts{}},
		{"broken", "mail", foreman.InsertOpts{}},
	} {
		opts := a.opts
		opts.Queue = a.queue
		_, err := c.Insert(ctx, reportArgs{Name: a.name}, &opts)
		require.NoError(t, err)
		clk.Advance(time.Second)
	}
	_, err = c.JobWork(ctx, 4)
	require.NoError(t, err)
	_, err = c.JobWork(ctx, 5)
	require.NoError(t, err)
	_, err = pool.Exec(ctx, `INSERT INTO "`+schema+`".foreman_leader VALUES ('default', 'worker-1', $1, $2)`,
		clk.Now(), clk.Now().Add(15*time.Second))
	require.NoError(t, err)
	_, err = pool.Exec(ctx, `INSERT INTO "`+schema+`".foreman_periodic_ticks (name, tick_at, created_at, job_id) VALUES
		('nightly', $1, $1, 1), ('nightly', $2, $2, 2), ('marker-only', $2, $2, NULL)`,
		clk.Now().Add(-24*time.Hour), clk.Now())
	require.NoError(t, err)
	return map[string]string{"FOREMAN_DATABASE_URL": db.URL, "FOREMAN_SCHEMA": schema}
}

func golden(t *testing.T, name, got string) {
	t.Helper()
	path := filepath.Join("testdata", "golden", name+".golden")
	if *update {
		require.NoError(t, os.MkdirAll(filepath.Dir(path), 0o750))
		require.NoError(t, os.WriteFile(path, []byte(got), 0o600))
	}
	want, err := os.ReadFile(path)
	require.NoError(t, err, "run with -update to create %s", path)
	assert.Equal(t, string(want), got)
}

func TestCLI_GoldenOutput(t *testing.T) {
	t.Parallel()
	env := seed(t)
	cases := []struct {
		name string
		args []string
	}{
		{"jobs_list", []string{"jobs", "list"}},
		{"jobs_list_filtered", []string{"jobs", "list", "--queue", "reports", "--state", "available", "--limit", "1"}},
		{"jobs_get", []string{"jobs", "get", "2"}},
		{"jobs_get_discarded", []string{"jobs", "get", "5"}},
		{"jobs_get_json", []string{"jobs", "get", "1", "-o", "json"}},
		{"queues_list", []string{"queues", "list"}},
		{"leader", []string{"leader"}},
		{"periodic_list", []string{"periodic", "list"}},
		{"stats", []string{"stats"}},
		{"migrate_version", []string{"migrate", "version"}},
	}
	for _, tc := range cases {
		code, out, errOut := run(env, tc.args...)
		require.Equal(t, cli.ExitOK, code, "%s: %s", tc.name, errOut)
		golden(t, tc.name, out)
	}
}

func TestCLI_MutatingCommands(t *testing.T) {
	t.Parallel()
	env := seed(t)
	steps := []struct {
		args []string
		code int
		out  string
	}{
		{[]string{"jobs", "cancel", "1"}, cli.ExitOK, "job 1 cancelled\n"},
		{[]string{"jobs", "retry", "1"}, cli.ExitOK, "job 1 available\n"},
		{[]string{"jobs", "discard", "1"}, cli.ExitOK, "job 1 discarded\n"},
		{[]string{"jobs", "cancel", "4"}, cli.ExitError, ""},
		{[]string{"jobs", "get", "999"}, cli.ExitError, ""},
		{[]string{"queues", "pause", "reports"}, cli.ExitOK, "queue reports paused\n"},
		{[]string{"queues", "resume", "reports"}, cli.ExitOK, "queue reports resumed\n"},
		{[]string{"jobs", "retry", "5", "-o", "json"}, cli.ExitOK, `"State": "available"`},
		{[]string{"migrate", "up"}, cli.ExitOK, "at version 1\n"},
		{[]string{"leader", "-o", "json"}, cli.ExitOK, `"LeaderID": "worker-1"`},
		{[]string{"stats", "-o", "json"}, cli.ExitOK, `"queues": 2`},
		{[]string{"queues", "list", "-o", "json"}, cli.ExitOK, `"Name": "mail"`},
		{[]string{"periodic", "list", "-o", "json"}, cli.ExitOK, `"Name": "nightly"`},
		{[]string{"jobs", "list", "-o", "json"}, cli.ExitOK, `"Jobs"`},
		{[]string{"migrate", "version", "-o", "json"}, cli.ExitOK, `"version": 1`},
		{[]string{"migrate", "down"}, cli.ExitOK, "at version 0\n"},
		{[]string{"leader"}, cli.ExitError, ""},
	}
	for _, s := range steps {
		code, out, errOut := run(env, s.args...)
		assert.Equal(t, s.code, code, "%v: %s", s.args, errOut)
		assert.Contains(t, out, s.out, "%v", s.args)
	}
	code, out, _ := run(env, "migrate", "up")
	require.Equal(t, cli.ExitOK, code)
	assert.Contains(t, out, "at version 1")
	code, out, _ = run(env, "leader")
	require.Equal(t, cli.ExitOK, code)
	assert.Equal(t, "no leader\n", out)
	code, out, _ = run(env, "leader", "-o", "json")
	require.Equal(t, cli.ExitOK, code)
	assert.JSONEq(t, `{"leader_id":null}`, out)
}
