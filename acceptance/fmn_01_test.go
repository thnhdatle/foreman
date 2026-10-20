// SPDX-License-Identifier: Apache-2.0

//go:build acceptance

package acceptance

import (
	"context"
	"fmt"
	"math/rand/v2"
	"strconv"
	"sync"
	"syscall"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/thnhdatle/foreman"
	"github.com/thnhdatle/foreman/acceptance/internal/accjobs"
)

// FMN-01: with worker processes and the leader killed with kill -9 every
// 20-60 s, every job ends completed or discarded, and a job runs more than
// once only when it was rescued.
func TestAcceptance_FMN01_NoLostJobs(t *testing.T) {
	th := loadThresholds(t, "FMN-01")
	total, nProcs := th.Int(t, "jobs"), th.Int(t, "client_processes")
	workers := th.Int(t, "workers_per_client")
	killMin := time.Duration(th.Int(t, "kill_interval_min_s")) * time.Second
	killMax := time.Duration(th.Int(t, "kill_interval_max_s")) * time.Second
	heartbeat := time.Duration(th.Int(t, "heartbeat_interval_s")) * time.Second
	rescueAfter := time.Duration(th.Int(t, "rescue_stuck_jobs_after_s")) * time.Second
	ctx := context.Background()
	const schema = "acc_fmn01"
	pool := newPool(t, dbURL(), 20)
	freshSchema(t, pool, schema)
	admin, err := foreman.NewClient(pool, foreman.Config{Schema: schema, Logger: testLogger(t)})
	require.NoError(t, err)

	// Jobs take some time so that the run spans several kill intervals at any
	// size: capacity x run time / jobs, with +-50% jitter (about 4 minutes).
	const targetRun = 4 * time.Minute
	meanMS := int(time.Duration(int64(targetRun) * int64(nProcs*workers) / int64(total)).Milliseconds())
	jitter := rand.New(rand.NewPCG(2, 2))
	insertStart := time.Now()
	const batch = 10_000
	params := make([]foreman.InsertParams, 0, batch)
	for i := 0; i < total; i++ {
		sleep := meanMS/2 + jitter.IntN(meanMS+1)
		params = append(params, foreman.InsertParams{Args: accjobs.NoopArgs{Seq: int64(i), SleepMS: sleep}})
		if len(params) == batch || i == total-1 {
			_, err := admin.InsertMany(ctx, params)
			require.NoError(t, err)
			params = params[:0]
		}
	}
	t.Logf("inserted %d jobs in %s", total, time.Since(insertStart))

	args := func(id string) []string {
		return []string{
			"-database-url", dbURL(), "-schema", schema, "-id", id,
			"-queues", "default:" + strconv.Itoa(workers), "-heartbeat", heartbeat.String(), "-rescue-after", rescueAfter.String(),
		}
	}
	var mu sync.Mutex
	procs := make([]*proc, nProcs)
	next := 0
	for i := range procs {
		procs[i] = startWorker(t, args(fmt.Sprintf("p%d", next))...)
		next++
	}

	t.Cleanup(func() { // workers restarted by the killer are not registered with the test
		mu.Lock()
		defer mu.Unlock()
		for _, p := range procs {
			p.kill()
		}
	})
	// Killer: alternate between the leader and a random worker.
	bin := buildWorker(t)
	var spawnErr error
	stop := make(chan struct{})
	killerDone := make(chan struct{})
	kills, leaderKills := 0, 0
	rng := rand.New(rand.NewPCG(1, 1))
	go func() {
		defer close(killerDone)
		for {
			wait := killMin + time.Duration(rng.Int64N(int64(killMax-killMin)+1))
			select {
			case <-stop:
				return
			case <-time.After(wait):
			}
			mu.Lock()
			victim := rng.IntN(nProcs)
			if kills%2 == 0 {
				var leaderID string
				if err := pool.QueryRow(ctx, `SELECT leader_id FROM "acc_fmn01".foreman_leader`).Scan(&leaderID); err == nil {
					for i, p := range procs {
						if p.id == leaderID {
							victim = i
							leaderKills++
						}
					}
				}
			}
			procs[victim].signal(syscall.SIGKILL)
			procs[victim].wait(10 * time.Second)
			kills++
			p, err := spawnWorker(bin, args(fmt.Sprintf("p%d", next))...)
			if err != nil {
				spawnErr = err
				mu.Unlock()
				return
			}
			procs[victim] = p
			next++
			mu.Unlock()
		}
	}()

	runStart := time.Now()
	finished := waitFor(90*time.Minute, time.Second, func() bool {
		return countWhere(t, pool, schema, "state IN ('completed', 'discarded')") == int64(total)
	})
	elapsed := time.Since(runStart)
	close(stop)
	<-killerDone
	require.NoError(t, spawnErr, "restarting a killed worker")
	mu.Lock()
	for _, p := range procs {
		p.stop(30 * time.Second)
	}
	mu.Unlock()

	final := countWhere(t, pool, schema, "state IN ('completed', 'discarded')")
	discarded := countWhere(t, pool, schema, "state = 'discarded'")
	rescued := countWhere(t, pool, schema, "array_length(errors, 1) > 0")
	var duplicated, unexplained, executions int64
	require.NoError(t, pool.QueryRow(ctx, `SELECT count(*) FROM "acc_fmn01".acc_executions`).Scan(&executions))
	require.NoError(t, pool.QueryRow(ctx, `
		WITH e AS (SELECT job_id, count(*) AS n FROM "acc_fmn01".acc_executions GROUP BY job_id HAVING count(*) > 1)
		SELECT count(*), count(*) FILTER (WHERE (
			SELECT count(*) FROM unnest(j.errors) AS x WHERE x->>'error' LIKE 'rescued:%') < e.n - 1)
		FROM e JOIN "acc_fmn01".foreman_jobs j ON j.id = e.job_id`).Scan(&duplicated, &unexplained))
	lost := int64(total) - final
	ratio := float64(final) / float64(total)
	measured := map[string]any{
		"jobs": total, "finalized": final, "finalized_ratio": ratio, "lost": lost, "discarded": discarded,
		"kills": kills, "leader_kills": leaderKills, "jobs_rescued": rescued, "executions": executions,
		"jobs_executed_more_than_once": duplicated, "unexplained_duplicate_executions": unexplained,
		"elapsed_s": elapsed.Seconds(), "jobs_per_s": float64(total) / elapsed.Seconds(), "mean_job_ms": meanMS,
	}
	pass := finished && lost <= int64(th.Int(t, "lost_max")) && ratio >= th.Float(t, "finalized_ratio_min") &&
		unexplained <= int64(th.Int(t, "unexplained_duplicate_executions_max")) && kills > 0
	writeResult(t, "FMN-01", measured, pass, nil)
	require.True(t, finished, "all jobs finalized")
	assert.LessOrEqual(t, lost, int64(th.Int(t, "lost_max")))
	assert.LessOrEqual(t, unexplained, int64(th.Int(t, "unexplained_duplicate_executions_max")))
	assert.Positive(t, kills, "processes were killed during the run")
}
