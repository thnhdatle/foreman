// SPDX-License-Identifier: Apache-2.0

package cli

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"runtime"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/spf13/cobra"

	"github.com/thnhdatle/foreman"
	"github.com/thnhdatle/foreman/internal/config"
)

const (
	ExitOK     = 0
	ExitError  = 1
	ExitConfig = 2
)

type BuildInfo struct {
	Version, Commit, BuildDate string
}

// IO holds the process streams and environment.
type IO struct {
	Stdout, Stderr io.Writer
	Getenv         func(string) string
}

type app struct {
	io         IO
	build      BuildInfo
	configPath string
	flags      config.Config
	setFlags   map[string]bool
	cfg        config.Config
	pool       *pgxpool.Pool
}

type errConfig struct{ err error }

func (e errConfig) Error() string { return e.err.Error() }
func (e errConfig) Unwrap() error { return e.err }

// Run executes the CLI with args and returns the exit code.
func Run(ctx context.Context, args []string, io IO, build BuildInfo) int {
	a := &app{io: io, build: build, setFlags: map[string]bool{}}
	root := a.root() //nolint:contextcheck // cobra commands take their context from cmd.Context()
	root.SetArgs(args)
	root.SetOut(io.Stdout)
	root.SetErr(io.Stderr)
	err := root.ExecuteContext(ctx)
	if a.pool != nil {
		a.pool.Close()
	}
	if err == nil {
		return ExitOK
	}
	_, _ = fmt.Fprintln(io.Stderr, "error:", err)
	var ce errConfig
	if errors.As(err, &ce) {
		return ExitConfig
	}
	return ExitError
}

func (a *app) root() *cobra.Command {
	root := &cobra.Command{
		Use:           "foreman",
		Short:         "Administer Foreman job queues in Postgres",
		SilenceUsage:  true,
		SilenceErrors: true,
	}
	pf := root.PersistentFlags()
	pf.StringVar(&a.configPath, "config", "", "config file (env FOREMAN_CONFIG)")
	pf.StringVar(&a.flags.DatabaseURL, "database-url", "", "Postgres URL (env FOREMAN_DATABASE_URL)")
	pf.StringVar(&a.flags.Schema, "schema", "", "schema holding the Foreman tables (default public)")
	pf.StringVarP(&a.flags.Output, "output", "o", "", "output format: text or json (default text)")
	root.AddCommand(a.migrateCmd(), a.jobsCmd(), a.queuesCmd(), a.leaderCmd(), a.periodicCmd(), a.statsCmd(),
		a.configCmd(), a.versionCmd())
	return root
}

func (a *app) load(cmd *cobra.Command, needDatabase bool) error {
	cfg, err := config.Load(a.configPath, a.io.Getenv)
	if err != nil {
		return errConfig{err}
	}
	pf := cmd.Flags()
	if pf.Changed("database-url") {
		cfg.DatabaseURL = a.flags.DatabaseURL
	}
	if pf.Changed("schema") {
		cfg.Schema = a.flags.Schema
	}
	if pf.Changed("output") {
		cfg.Output = a.flags.Output
	}
	if err := cfg.ResolveSecrets(); err != nil {
		return errConfig{err}
	}
	if err := cfg.Validate(needDatabase); err != nil {
		return errConfig{err}
	}
	a.cfg = cfg
	return nil
}

func (a *app) connect(ctx context.Context) (*pgxpool.Pool, error) {
	if a.pool != nil {
		return a.pool, nil
	}
	pcfg, err := pgxpool.ParseConfig(a.cfg.DatabaseURL)
	if err != nil {
		return nil, errConfig{fmt.Errorf("database URL: %w", err)}
	}
	pcfg.MaxConns = 4
	pool, err := pgxpool.NewWithConfig(ctx, pcfg)
	if err != nil {
		return nil, fmt.Errorf("connect: %w", err)
	}
	a.pool = pool
	return pool, nil
}

func (a *app) client(ctx context.Context) (*foreman.Client, error) {
	pool, err := a.connect(ctx)
	if err != nil {
		return nil, err
	}
	level := map[string]slog.Level{
		"debug": slog.LevelDebug, "info": slog.LevelInfo, "warn": slog.LevelWarn,
		"error": slog.LevelError,
	}[a.cfg.Log.Level]
	var h slog.Handler = slog.NewTextHandler(a.io.Stderr, &slog.HandlerOptions{Level: level})
	if a.cfg.Log.Format == "json" {
		h = slog.NewJSONHandler(a.io.Stderr, &slog.HandlerOptions{Level: level})
	}
	c, err := foreman.NewClient(pool, foreman.Config{Schema: a.cfg.Schema, Logger: slog.New(h)})
	if err != nil {
		return nil, errConfig{err}
	}
	return c, nil
}

func (a *app) withClient(fn func(ctx context.Context, c *foreman.Client) error) func(*cobra.Command, []string) error {
	return func(cmd *cobra.Command, _ []string) error {
		if err := a.load(cmd, true); err != nil {
			return err
		}
		ctx, cancel := context.WithTimeout(cmd.Context(), a.cfg.Timeout.Std())
		defer cancel()
		c, err := a.client(ctx)
		if err != nil {
			return err
		}
		return fn(ctx, c)
	}
}

func (a *app) json() bool { return a.cfg.Output == "json" }

func (a *app) printJSON(v any) error {
	enc := json.NewEncoder(a.io.Stdout)
	enc.SetIndent("", "  ")
	if err := enc.Encode(v); err != nil {
		return fmt.Errorf("write output: %w", err)
	}
	return nil
}

func (a *app) printf(format string, args ...any) {
	_, _ = fmt.Fprintf(a.io.Stdout, format, args...)
}

func (a *app) versionCmd() *cobra.Command {
	return &cobra.Command{
		Use:   "version",
		Short: "Print the version",
		Args:  cobra.NoArgs,
		RunE: func(*cobra.Command, []string) error {
			a.printf("foreman %s (commit %s, built %s, %s)\n", a.build.Version, a.build.Commit, a.build.BuildDate,
				runtime.Version())
			return nil
		},
	}
}

func (a *app) configCmd() *cobra.Command {
	cmd := &cobra.Command{Use: "config", Short: "Validate or print the configuration"}
	cmd.AddCommand(&cobra.Command{
		Use:   "validate",
		Short: "Check the configuration and report every problem",
		Args:  cobra.NoArgs,
		RunE: func(cmd *cobra.Command, _ []string) error {
			if err := a.load(cmd, false); err != nil {
				return err
			}
			a.printf("config ok\n")
			return nil
		},
	})
	var redact bool
	printCmd := &cobra.Command{
		Use:   "print",
		Short: "Print the effective configuration",
		Args:  cobra.NoArgs,
		RunE: func(cmd *cobra.Command, _ []string) error {
			if err := a.load(cmd, false); err != nil {
				return err
			}
			if !redact {
				return errConfig{errors.New("config print requires --redact")}
			}
			out, err := a.cfg.Redacted()
			if err != nil {
				return err //nolint:wrapcheck // already prefixed
			}
			a.printf("%s", out)
			return nil
		},
	}
	printCmd.Flags().BoolVar(&redact, "redact", false, "hide secrets (required)")
	cmd.AddCommand(printCmd)
	return cmd
}
