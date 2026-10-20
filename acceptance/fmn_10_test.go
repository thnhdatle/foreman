// SPDX-License-Identifier: Apache-2.0

//go:build acceptance

package acceptance

import (
	"context"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/thnhdatle/foreman"
)

type lossArgs struct {
	N int `json:"n"`
}

func (lossArgs) Kind() string { return "acc_loss" }

// FMN-10: clients connected through Toxiproxy survive a 30 s connection cut
// under load without restart: no job is lost and every job that ran has its
// completion recorded.
func TestAcceptance_FMN10_ConnectionLoss(t *testing.T) {
	th := loadThresholds(t, "FMN-10")
	outage := time.Duration(th.Int(t, "outage_s")) * time.Second
	rescueAfter := time.Duration(th.Int(t, "rescue_stuck_jobs_after_s")) * time.Second
	nClients, rate := th.Int(t, "clients"), th.Int(t, "load_jobs_per_s")
	before := time.Duration(th.Int(t, "load_before_s")) * time.Second
	after := time.Duration(th.Int(t, "load_after_s")) * time.Second
	ctx := context.Background()
	const schema = "acc_fmn10"
	direct := newPool(t, dbURL(), 20)
	freshSchema(t, direct, schema)
	toxiproxySetEnabled(t, true)
	t.Cleanup(func() { toxiproxySetEnabled(t, true) })

	var mu sync.Mutex
	runs := map[int64]int{}
	ranAfterRestore := map[string]int{}
	var restoredAt time.Time
	workers := foreman.NewWorkers()
	foreman.AddWorker(workers, foreman.WorkFunc[lossArgs](func(ctx context.Context, j *foreman.Job[lossArgs]) error {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(20 * time.Millisecond):
		}
		mu.Lock()
		runs[j.ID]++
		if !restoredAt.IsZero() && time.Now().After(restoredAt) {
			ranAfterRestore[foreman.ClientFromContext(ctx).ID()]++
		}
		mu.Unlock()
		return nil
	}))
	clients := make([]*foreman.Client, nClients)
	for i := range clients {
		pool := newPool(t, proxyURL(), 20)
		c, err := foreman.NewClient(pool, foreman.Config{
			Schema: schema, Workers: workers, Logger: testLogger(t), ID: fmt.Sprintf("client-%d", i),
			RescueStuckJobsAfter: rescueAfter,
			Queues:               map[string]foreman.QueueConfig{"default": {MaxWorkers: 20, FetchInterval: 500 * time.Millisecond}},
		})
		require.NoError(t, err)
		require.NoError(t, c.Start(ctx))
		clients[i] = c
	}
	t.Cleanup(func() {
		for _, c := range clients {
			sctx, cancel := context.WithTimeout(context.Background(), time.Minute)
			_ = c.StopAndCancel(sctx)
			cancel()
		}
	})

	// The load comes through a direct connection, so it continues during the cut.
	loader, err := foreman.NewClient(direct, foreman.Config{Schema: schema, Logger: testLogger(t)})
	require.NoError(t, err)
	inserted := 0
	loadDone := make(chan error, 1)
	go func() {
		tick := time.NewTicker(100 * time.Millisecond)
		defer tick.Stop()
		end := time.Now().Add(before + outage + after)
		for time.Now().Before(end) {
			<-tick.C
			params := make([]foreman.InsertParams, rate/10)
			for i := range params {
				params[i] = foreman.InsertParams{Args: lossArgs{N: inserted + i}}
			}
			if _, err := loader.InsertMany(ctx, params); err != nil {
				loadDone <- err
				return
			}
			inserted += len(params)
		}
		loadDone <- nil
	}()

	<-time.After(before)
	toxiproxySetEnabled(t, false)
	cutAt := time.Now()
	<-time.After(outage)
	toxiproxySetEnabled(t, true)
	mu.Lock()
	restoredAt = time.Now()
	mu.Unlock()
	require.NoError(t, <-loadDone)

	total := int64(inserted)
	// Jobs whose fetch committed while the client lost the reply are rescued
	// after RescueStuckJobsAfter, so allow for that.
	finished := waitFor(rescueAfter+3*time.Minute, 500*time.Millisecond, func() bool {
		return countWhere(t, direct, schema, "state = 'completed'") == total
	})
	completed := countWhere(t, direct, schema, "state = 'completed'")
	mu.Lock()
	distinctRun := int64(len(runs))
	executions := 0
	for _, n := range runs {
		executions += n
	}
	recovered := len(ranAfterRestore)
	afterRestore := map[string]int{}
	for k, v := range ranAfterRestore {
		afterRestore[k] = v
	}
	mu.Unlock()
	lost := total - completed
	mismatch := distinctRun - completed
	if mismatch < 0 {
		mismatch = -mismatch
	}
	measured := map[string]any{
		"jobs": total, "completed": completed, "lost": lost, "jobs_run": distinctRun, "executions": executions,
		"completion_mismatch": mismatch, "clients_recovered": recovered, "clients": nClients,
		"ran_after_restore": afterRestore, "outage_s": restoredAt.Sub(cutAt).Seconds(),
		"rescued": countWhere(t, direct, schema, "array_length(errors, 1) > 0"),
	}
	pass := finished && lost <= int64(th.Int(t, "lost_max")) &&
		mismatch <= int64(th.Int(t, "completion_mismatch_max")) && recovered == nClients
	writeResult(t, "FMN-10", measured, pass, nil)
	require.True(t, finished, "all jobs completed")
	assert.LessOrEqual(t, lost, int64(th.Int(t, "lost_max")))
	assert.LessOrEqual(t, mismatch, int64(th.Int(t, "completion_mismatch_max")))
	assert.Equal(t, nClients, recovered, "every client ran jobs after the connection came back, without restart")
}
