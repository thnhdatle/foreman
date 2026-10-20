// SPDX-License-Identifier: Apache-2.0

//go:build acceptance

package acceptance

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/thnhdatle/foreman"
	"github.com/thnhdatle/foreman/foremantest"
)

type periodicArgs struct {
	Every string `json:"every"`
}

func (periodicArgs) Kind() string { return "acc_periodic" }

const fmn02Schema = "acc_fmn02"

// FMN-02: periodic jobs are enqueued exactly once per tick over 24 simulated
// hours, while the leader is crashed (without resigning) every 10 simulated
// minutes and replaced by a new client.
func TestAcceptance_FMN02_PeriodicExactlyOnce(t *testing.T) {
	th := loadThresholds(t, "FMN-02")
	simulated := time.Duration(th.Int(t, "simulated_hours")) * time.Hour
	realMax := time.Duration(th.Int(t, "real_minutes_max")) * time.Minute
	nClients := th.Int(t, "clients")
	crashEvery := time.Duration(th.Int(t, "crash_every_simulated_min")) * time.Minute
	catchUp := time.Duration(th.Int(t, "catch_up_min")) * time.Minute
	ctx := context.Background()
	pool := newPool(t, dbURL(), 80)
	freshSchema(t, pool, fmn02Schema)

	start := time.Date(2026, 1, 1, 0, 0, 30, 0, time.UTC)
	clk := foremantest.NewClock(start)
	newClient := func() *foreman.Client {
		workers := foreman.NewWorkers()
		foreman.AddWorker(workers, foreman.WorkFunc[periodicArgs](func(context.Context, *foreman.Job[periodicArgs]) error {
			return nil
		}))
		c, err := foreman.NewClient(pool, foreman.Config{
			Schema: fmn02Schema, Clock: clk, Workers: workers, Logger: testLogger(t),
			CompletedJobRetention: -1, // keep every job to count duplicates
			Queues:                map[string]foreman.QueueConfig{"default": {MaxWorkers: 5}},
			PeriodicJobs: []*foreman.PeriodicJob{
				{
					Name: "every-minute", Schedule: foreman.MustCron("* * * * *"), CatchUp: catchUp,
					Args: func() (foreman.JobArgs, *foreman.InsertOpts) { return periodicArgs{Every: "minute"}, nil },
				},
				{
					Name: "hourly", Schedule: foreman.MustCron("0 * * * *"), CatchUp: catchUp,
					Args: func() (foreman.JobArgs, *foreman.InsertOpts) { return periodicArgs{Every: "hour"}, nil },
				},
			},
		})
		require.NoError(t, err)
		require.NoError(t, c.Start(ctx))
		return c
	}
	clients := make([]*foreman.Client, nClients)
	for i := range clients {
		clients[i] = newClient()
	}
	var stopping sync.WaitGroup
	stopLater := func(c *foreman.Client) {
		stopping.Add(1)
		go func() {
			defer stopping.Done()
			sctx, cancel := context.WithTimeout(context.Background(), time.Minute)
			defer cancel()
			_ = c.StopAndCancel(sctx)
		}()
	}

	realStart := time.Now()
	const step = 2 * time.Second
	pace := time.NewTicker(3 * time.Millisecond)
	defer pace.Stop()
	crashes := 0
	nextCrash := start.Add(crashEvery)
	end := start.Add(simulated)
	for clk.Now().Before(end.Add(2 * time.Minute)) { // 2 extra minutes let the last ticks be enqueued
		<-pace.C
		clk.Advance(step)
		now := clk.Now()
		if now.Truncate(time.Minute).Equal(now) {
			// Keep the simulation coherent: let the leader enqueue the tick of this minute.
			waitFor(2*time.Second, time.Millisecond, func() bool { return tickExists(ctx, pool, "every-minute", now) })
		}
		if !now.Before(nextCrash) && now.Before(end) {
			for i, c := range clients {
				if c.IsLeader() {
					foreman.CrashForTest(c)
					stopLater(c)
					clients[i] = newClient()
					crashes++
					break
				}
			}
			nextCrash = nextCrash.Add(crashEvery)
		}
	}
	realElapsed := time.Since(realStart)
	for _, c := range clients {
		stopLater(c)
	}
	stopping.Wait()

	missedMinute, dupMinute := tickStats(t, pool, "every-minute", time.Minute, start, end)
	missedHour, dupHour := tickStats(t, pool, "hourly", time.Hour, start, end)
	missed, dups := missedMinute+missedHour, dupMinute+dupHour
	measured := map[string]any{
		"simulated_hours": simulated.Hours(), "real_seconds": realElapsed.Seconds(), "crashes": crashes,
		"missed_ticks": missed, "duplicate_ticks": dups, "missed_minute": missedMinute, "missed_hourly": missedHour,
		"duplicate_minute": dupMinute, "duplicate_hourly": dupHour,
	}
	pass := missed <= th.Int(t, "missed_ticks_max") && dups <= th.Int(t, "duplicate_ticks_max") && realElapsed <= realMax
	writeResult(t, "FMN-02", measured, pass, nil)
	assert.LessOrEqual(t, dups, th.Int(t, "duplicate_ticks_max"))
	assert.LessOrEqual(t, missed, th.Int(t, "missed_ticks_max"))
	assert.LessOrEqual(t, realElapsed, realMax)
	assert.GreaterOrEqual(t, crashes, int(simulated/crashEvery)-1)
}

func tickExists(ctx context.Context, pool *pgxpool.Pool, name string, at time.Time) bool {
	var ok bool
	err := pool.QueryRow(ctx, `SELECT EXISTS (SELECT 1 FROM "acc_fmn02".foreman_periodic_ticks
		WHERE name = $1 AND tick_at = $2 AND job_id IS NOT NULL)`, name, at).Scan(&ok)
	return err == nil && ok
}

// tickStats counts the ticks of every period in (start, end] that have no
// job, and the ticks with more than one job.
func tickStats(t *testing.T, pool *pgxpool.Pool, name string, period time.Duration, start, end time.Time) (missed, dups int) {
	t.Helper()
	ctx := context.Background()
	rows, err := pool.Query(ctx, `SELECT metadata->>'foreman_tick_at', count(*) FROM "acc_fmn02".foreman_jobs
		WHERE metadata->>'foreman_periodic' = $1 GROUP BY 1`, name)
	require.NoError(t, err)
	jobs := map[time.Time]int{}
	for rows.Next() {
		var at string
		var n int
		require.NoError(t, rows.Scan(&at, &n))
		ts, err := time.Parse(time.RFC3339Nano, at)
		require.NoError(t, err)
		jobs[ts.UTC()] = n
	}
	require.NoError(t, rows.Err())
	for tick := start.Truncate(period).Add(period); !tick.After(end); tick = tick.Add(period) {
		switch n := jobs[tick]; {
		case n == 0:
			missed++
		case n > 1:
			dups += n - 1
		}
	}
	return missed, dups
}
