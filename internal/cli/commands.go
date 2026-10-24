// SPDX-License-Identifier: Apache-2.0

package cli

import (
	"context"
	"errors"
	"fmt"
	"strconv"
	"strings"
	"text/tabwriter"
	"time"

	"github.com/spf13/cobra"

	"github.com/thnhdatle/foreman"
	"github.com/thnhdatle/foreman/migrate"
)

const timeLayout = time.RFC3339

func ts(t time.Time) string { return t.UTC().Format(timeLayout) }

func tsPtr(t *time.Time) string {
	if t == nil {
		return "-"
	}
	return ts(*t)
}

func (a *app) table(header string, rows [][]string) {
	w := tabwriter.NewWriter(a.io.Stdout, 0, 2, 2, ' ', 0)
	_, _ = fmt.Fprintln(w, header)
	for _, r := range rows {
		_, _ = fmt.Fprintln(w, strings.Join(r, "\t"))
	}
	_ = w.Flush()
}

func (a *app) migrateCmd() *cobra.Command {
	cmd := &cobra.Command{Use: "migrate", Short: "Install, remove or inspect the Foreman schema"}
	cmd.AddCommand(
		&cobra.Command{Use: "up", Short: "Apply pending migrations", Args: cobra.NoArgs, RunE: a.migrateRun(a.migrateUp)},
		&cobra.Command{
			Use: "down", Short: "Revert every migration (removes all Foreman objects)", Args: cobra.NoArgs,
			RunE: a.migrateRun(a.migrateDown),
		},
		&cobra.Command{
			Use: "version", Short: "Print the schema version", Args: cobra.NoArgs, RunE: a.migrateRun(a.migrateVersion),
		},
		&cobra.Command{
			Use: "sql [up|down]", Short: "Print the migration SQL for your own migration tool", Args: cobra.MaximumNArgs(1),
			RunE: a.migrateSQL,
		},
	)
	return cmd
}

func (a *app) migrateRun(fn func(ctx context.Context, opt migrate.Option) error) func(*cobra.Command, []string) error {
	return func(cmd *cobra.Command, _ []string) error {
		if err := a.load(cmd, true); err != nil {
			return err
		}
		ctx, cancel := context.WithTimeout(cmd.Context(), a.cfg.Timeout.Std())
		defer cancel()
		if _, err := a.connect(ctx); err != nil {
			return err
		}
		return fn(ctx, migrate.WithSchema(a.cfg.Schema))
	}
}

func (a *app) migrateUp(ctx context.Context, opt migrate.Option) error {
	if err := migrate.Up(ctx, a.pool, opt); err != nil {
		return err //nolint:wrapcheck // migrate errors are prefixed
	}
	a.printf("schema %s at version %d\n", a.cfg.Schema, migrate.Latest())
	return nil
}

func (a *app) migrateDown(ctx context.Context, opt migrate.Option) error {
	if err := migrate.Down(ctx, a.pool, opt); err != nil {
		return err //nolint:wrapcheck // migrate errors are prefixed
	}
	a.printf("schema %s at version 0\n", a.cfg.Schema)
	return nil
}

func (a *app) migrateVersion(ctx context.Context, opt migrate.Option) error {
	v, err := migrate.Version(ctx, a.pool, opt)
	if err != nil {
		return err //nolint:wrapcheck // migrate errors are prefixed
	}
	if a.json() {
		return a.printJSON(map[string]int{"version": v, "latest": migrate.Latest()})
	}
	a.printf("version %d (latest %d)\n", v, migrate.Latest())
	return nil
}

func (a *app) migrateSQL(cmd *cobra.Command, args []string) error {
	if err := a.load(cmd, false); err != nil {
		return err
	}
	if len(args) == 1 && args[0] != "up" && args[0] != "down" {
		return errConfig{fmt.Errorf("direction must be up or down, not %q", args[0])}
	}
	sql, err := migrate.SQL(len(args) == 0 || args[0] == "up", migrate.WithSchema(a.cfg.Schema))
	if err != nil {
		return err //nolint:wrapcheck // migrate errors are prefixed
	}
	a.printf("%s", sql)
	return nil
}

func parseID(s string) (int64, error) {
	id, err := strconv.ParseInt(s, 10, 64)
	if err != nil || id <= 0 {
		return 0, errConfig{fmt.Errorf("job id must be a positive integer, got %q", s)}
	}
	return id, nil
}

func (a *app) jobsCmd() *cobra.Command {
	cmd := &cobra.Command{Use: "jobs", Short: "List, inspect and change jobs"}
	var states, kinds, queues []string
	var limit int
	var cursor string
	list := &cobra.Command{
		Use: "list", Short: "List jobs, newest first", Args: cobra.NoArgs,
		RunE: a.withClient(func(ctx context.Context, c *foreman.Client) error {
			p := &foreman.JobListParams{Kinds: kinds, Queues: queues, Limit: limit, Cursor: cursor}
			for _, s := range states {
				p.States = append(p.States, foreman.State(s))
			}
			res, err := c.JobList(ctx, p)
			if err != nil {
				return err //nolint:wrapcheck // client errors are prefixed
			}
			if a.json() {
				return a.printJSON(res)
			}
			rows := make([][]string, len(res.Jobs))
			for i, j := range res.Jobs {
				rows[i] = []string{
					strconv.FormatInt(j.ID, 10), j.Kind, j.Queue, string(j.State),
					fmt.Sprintf("%d/%d", j.Attempt, j.MaxAttempts), strconv.Itoa(j.Priority), ts(j.ScheduledAt),
				}
			}
			a.table("ID\tKIND\tQUEUE\tSTATE\tATTEMPT\tPRIORITY\tSCHEDULED_AT", rows)
			if res.NextCursor != "" {
				a.printf("next cursor: %s\n", res.NextCursor)
			}
			return nil
		}),
	}
	f := list.Flags()
	f.StringSliceVar(&states, "state", nil, "filter by state (repeatable)")
	f.StringSliceVar(&kinds, "kind", nil, "filter by kind (repeatable)")
	f.StringSliceVar(&queues, "queue", nil, "filter by queue (repeatable)")
	f.IntVar(&limit, "limit", foreman.DefaultListLimit, "page size (max 500)")
	f.StringVar(&cursor, "cursor", "", "cursor from a previous page")

	get := &cobra.Command{
		Use: "get <id>", Short: "Show one job", Args: cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			id, err := parseID(args[0])
			if err != nil {
				return err
			}
			return a.withClient(func(ctx context.Context, c *foreman.Client) error {
				j, err := c.JobGet(ctx, id)
				if err != nil {
					return err //nolint:wrapcheck // client errors are prefixed
				}
				return a.printJob(j)
			})(cmd, args)
		},
	}
	cmd.AddCommand(list, get,
		a.jobAction("retry", "Move a final job back to available", func(c *foreman.Client) jobFn { return c.JobRetry }),
		a.jobAction("cancel", "Cancel a job (running jobs: request)", func(c *foreman.Client) jobFn { return c.JobCancel }),
		a.jobAction("discard", "Discard a non-final job", func(c *foreman.Client) jobFn { return c.JobDiscard }))
	return cmd
}

type jobFn func(context.Context, int64) (*foreman.JobRow, error)

func (a *app) jobAction(name, short string, pick func(*foreman.Client) jobFn) *cobra.Command {
	return &cobra.Command{
		Use: name + " <id>", Short: short, Args: cobra.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			id, err := parseID(args[0])
			if err != nil {
				return err
			}
			return a.withClient(func(ctx context.Context, c *foreman.Client) error {
				j, err := pick(c)(ctx, id)
				if err != nil {
					return err
				}
				if a.json() {
					return a.printJSON(j)
				}
				a.printf("job %d %s\n", j.ID, j.State)
				return nil
			})(cmd, args)
		},
	}
}

func (a *app) printJob(j *foreman.JobRow) error {
	if a.json() {
		return a.printJSON(j)
	}
	a.printf("id:            %d\nkind:          %s\nqueue:         %s\nstate:         %s\n",
		j.ID, j.Kind, j.Queue, j.State)
	a.printf("attempt:       %d/%d\npriority:      %d\nscheduled_at:  %s\ncreated_at:    %s\n", j.Attempt,
		j.MaxAttempts, j.Priority, ts(j.ScheduledAt), ts(j.CreatedAt))
	a.printf("attempted_at:  %s\nfinalized_at:  %s\nattempted_by:  %s\n", tsPtr(j.AttemptedAt), tsPtr(j.FinalizedAt),
		strings.Join(j.AttemptedBy, ","))
	if j.ConcurrencyKey != "" {
		a.printf("concurrency:   %s\n", j.ConcurrencyKey)
	}
	if len(j.Tags) > 0 {
		a.printf("tags:          %s\n", strings.Join(j.Tags, ","))
	}
	a.printf("args:          %s\nmetadata:      %s\n", j.Args, j.Metadata)
	for _, e := range j.Errors {
		a.printf("error:         attempt %d at %s: %s\n", e.Attempt, ts(e.At), e.Error)
	}
	return nil
}

func (a *app) queuesCmd() *cobra.Command {
	cmd := &cobra.Command{Use: "queues", Short: "List, pause and resume queues"}
	cmd.AddCommand(&cobra.Command{
		Use: "list", Short: "List queues", Args: cobra.NoArgs,
		RunE: a.withClient(func(ctx context.Context, c *foreman.Client) error {
			qs, err := c.QueueList(ctx)
			if err != nil {
				return err //nolint:wrapcheck // client errors are prefixed
			}
			if a.json() {
				return a.printJSON(qs)
			}
			rows := make([][]string, len(qs))
			for i, q := range qs {
				rows[i] = []string{
					q.Name, strconv.FormatBool(q.Paused), strconv.FormatInt(q.Available, 10),
					strconv.FormatInt(q.Scheduled, 10), strconv.FormatInt(q.Running, 10),
					strconv.FormatInt(q.Retryable, 10), tsPtr(q.LastSeenAt),
				}
			}
			a.table("NAME\tPAUSED\tAVAILABLE\tSCHEDULED\tRUNNING\tRETRYABLE\tLAST_SEEN", rows)
			return nil
		}),
	})
	for _, pause := range []bool{true, false} {
		verb := map[bool]string{true: "pause", false: "resume"}[pause]
		cmd.AddCommand(&cobra.Command{
			Use: verb + " <name>", Short: verb + " a queue on every client", Args: cobra.ExactArgs(1),
			RunE: func(cmd *cobra.Command, args []string) error {
				return a.withClient(func(ctx context.Context, c *foreman.Client) error {
					fn := c.QueueResume
					if pause {
						fn = c.QueuePause
					}
					if err := fn(ctx, args[0]); err != nil {
						return err
					}
					a.printf("queue %s %sd\n", args[0], verb)
					return nil
				})(cmd, args)
			},
		})
	}
	return cmd
}

func (a *app) leaderCmd() *cobra.Command {
	return &cobra.Command{
		Use: "leader", Short: "Show the current leader", Args: cobra.NoArgs,
		RunE: a.withClient(func(ctx context.Context, c *foreman.Client) error {
			l, err := c.LeaderGet(ctx)
			if errors.Is(err, foreman.ErrNotFound) {
				if a.json() {
					return a.printJSON(map[string]any{"leader_id": nil})
				}
				a.printf("no leader\n")
				return nil
			}
			if err != nil {
				return err //nolint:wrapcheck // client errors are prefixed
			}
			if a.json() {
				return a.printJSON(l)
			}
			a.printf("leader_id:   %s\nelected_at:  %s\nexpires_at:  %s\n", l.LeaderID, ts(l.ElectedAt), ts(l.ExpiresAt))
			return nil
		}),
	}
}

func (a *app) periodicCmd() *cobra.Command {
	cmd := &cobra.Command{Use: "periodic", Short: "Inspect periodic jobs"}
	cmd.AddCommand(&cobra.Command{
		Use: "list", Short: "List periodic jobs and their last tick", Args: cobra.NoArgs,
		RunE: a.withClient(func(ctx context.Context, c *foreman.Client) error {
			ps, err := c.PeriodicList(ctx)
			if err != nil {
				return err //nolint:wrapcheck // client errors are prefixed
			}
			if a.json() {
				return a.printJSON(ps)
			}
			rows := make([][]string, len(ps))
			for i, p := range ps {
				rows[i] = []string{p.Name, ts(p.LastTick), strconv.FormatInt(p.Ticks, 10)}
			}
			a.table("NAME\tLAST_TICK\tTICKS", rows)
			return nil
		}),
	})
	return cmd
}

type stats struct {
	Queues    int                     `json:"queues"`
	Paused    int                     `json:"paused"`
	Available int64                   `json:"available"`
	Scheduled int64                   `json:"scheduled"`
	Running   int64                   `json:"running"`
	Retryable int64                   `json:"retryable"`
	Leader    *foreman.LeaderInfo     `json:"leader"`
	PerQueue  []*foreman.QueueInfo    `json:"per_queue"`
	Periodic  []*foreman.PeriodicInfo `json:"periodic"`
}

func (a *app) statsCmd() *cobra.Command {
	return &cobra.Command{
		Use: "stats", Short: "Summarise pending jobs, queues and the leader", Args: cobra.NoArgs,
		RunE: a.withClient(func(ctx context.Context, c *foreman.Client) error {
			qs, err := c.QueueList(ctx)
			if err != nil {
				return err //nolint:wrapcheck // client errors are prefixed
			}
			s := stats{Queues: len(qs), PerQueue: qs}
			for _, q := range qs {
				s.Available += q.Available
				s.Scheduled += q.Scheduled
				s.Running += q.Running
				s.Retryable += q.Retryable
				if q.Paused {
					s.Paused++
				}
			}
			if l, lerr := c.LeaderGet(ctx); lerr == nil {
				s.Leader = l
			} else if !errors.Is(lerr, foreman.ErrNotFound) {
				return lerr //nolint:wrapcheck // client errors are prefixed
			}
			if s.Periodic, err = c.PeriodicList(ctx); err != nil {
				return err //nolint:wrapcheck // client errors are prefixed
			}
			if a.json() {
				return a.printJSON(s)
			}
			leader := "none"
			if s.Leader != nil {
				leader = s.Leader.LeaderID
			}
			a.printf("queues:     %d (%d paused)\navailable:  %d\nscheduled:  %d\nrunning:    %d\nretryable:  %d\n",
				s.Queues, s.Paused, s.Available, s.Scheduled, s.Running, s.Retryable)
			a.printf("leader:     %s\nperiodic:   %d\n", leader, len(s.Periodic))
			return nil
		}),
	}
}
