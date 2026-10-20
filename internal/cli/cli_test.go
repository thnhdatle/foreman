// SPDX-License-Identifier: Apache-2.0

package cli_test

import (
	"bytes"
	"context"
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/thnhdatle/foreman/internal/cli"
)

func run(env map[string]string, args ...string) (code int, stdout, stderr string) {
	var out, errOut bytes.Buffer
	code = cli.Run(context.Background(), args, cli.IO{
		Stdout: &out, Stderr: &errOut, Getenv: func(k string) string { return env[k] },
	}, cli.BuildInfo{Version: "v0.1.0", Commit: "abc1234", BuildDate: "2026-10-18"})
	return code, out.String(), errOut.String()
}

func TestRun_CommandsWithoutDatabase(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name     string
		env      map[string]string
		args     []string
		code     int
		out, err string
	}{
		{name: "version", args: []string{"version"}, out: "foreman v0.1.0 (commit abc1234, built 2026-10-18, go"},
		{name: "config validate", args: []string{"config", "validate", "--config", "../../deploy/config.example.yaml"}, out: "config ok"},
		{
			name: "config validate reports all problems", env: map[string]string{"FOREMAN_OUTPUT": "xml", "FOREMAN_LOG_LEVEL": "loud"},
			args: []string{"config", "validate"}, code: cli.ExitConfig, err: "log.level",
		},
		{
			name: "config print redacts", env: map[string]string{"FOREMAN_DATABASE_URL": "postgres://u:hunter2@db/app"},
			args: []string{"config", "print", "--redact"}, out: "postgres://u:redacted@db/app",
		},
		{name: "config print needs redact", args: []string{"config", "print"}, code: cli.ExitConfig, err: "--redact"},
		{name: "missing database", args: []string{"jobs", "list"}, code: cli.ExitConfig, err: "database URL is required"},
		{
			name: "bad id", env: map[string]string{"FOREMAN_DATABASE_URL": "postgres://u@127.0.0.1:1/x"},
			args: []string{"jobs", "get", "abc"}, code: cli.ExitConfig, err: "positive integer",
		},
		{
			name: "bad id for action", env: map[string]string{"FOREMAN_DATABASE_URL": "postgres://u@127.0.0.1:1/x"},
			args: []string{"jobs", "retry", "0"}, code: cli.ExitConfig, err: "positive integer",
		},
		{name: "sql up", args: []string{"migrate", "sql", "--schema", "app"}, out: `CREATE TABLE IF NOT EXISTS "app".foreman_migrations`},
		{name: "sql down", args: []string{"migrate", "sql", "down"}, out: `DROP TABLE IF EXISTS "public".foreman_jobs`},
		{name: "sql bad direction", args: []string{"migrate", "sql", "sideways"}, code: cli.ExitConfig, err: "up or down"},
		{name: "unknown command", args: []string{"frobnicate"}, code: cli.ExitError, err: "unknown command"},
		{name: "bad config file", args: []string{"--config", "/does/not/exist", "config", "validate"}, code: cli.ExitConfig, err: "read"},
		{
			name: "database unreachable", env: map[string]string{"FOREMAN_DATABASE_URL": "postgres://u@127.0.0.1:1/x?connect_timeout=1"},
			args: []string{"queues", "list"}, code: cli.ExitError, err: "error:",
		},
		{
			name: "bad database url", env: map[string]string{"FOREMAN_DATABASE_URL": "postgres://u@127.0.0.1:1/x?connect_timeout=zz"},
			args: []string{"migrate", "up"}, code: cli.ExitConfig, err: "database URL",
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			code, out, errOut := run(tc.env, tc.args...)
			assert.Equal(t, tc.code, code, "stderr: %s", errOut)
			assert.Contains(t, out, tc.out)
			assert.Contains(t, errOut, tc.err)
		})
	}
}
