// SPDX-License-Identifier: Apache-2.0

//go:build acceptance

package acceptance

import (
	"context"
	"net"
	"strconv"
	"syscall"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/thnhdatle/foreman"
	"github.com/thnhdatle/foreman/acceptance/internal/accjobs"
)

func freeAddr(t *testing.T) string {
	t.Helper()
	ln, err := (&net.ListenConfig{}).Listen(context.Background(), "tcp", "127.0.0.1:0")
	require.NoError(t, err)
	addr := ln.Addr().String()
	require.NoError(t, ln.Close())
	return addr
}

// FMN-09: a non-leader worker process frozen with SIGSTOP mid-job has its job
// rescued and completed by another client; after SIGCONT its stale
// completion is rejected.
func TestAcceptance_FMN09_Rescue(t *testing.T) {
	th := loadThresholds(t, "FMN-09")
	rescueAfter := time.Duration(th.Int(t, "rescue_stuck_jobs_after_s")) * time.Second
	heartbeat := time.Duration(th.Int(t, "heartbeat_interval_s")) * time.Second
	bound := rescueAfter + time.Duration(th.Int(t, "rescue_within_heartbeats"))*heartbeat
	ctx := context.Background()
	const schema = "acc_fmn09"
	pool := newPool(t, dbURL(), 20)
	freshSchema(t, pool, schema)
	common := []string{
		"-database-url", dbURL(), "-schema", schema, "-heartbeat", heartbeat.String(),
		"-rescue-after", rescueAfter.String(),
	}

	// The leader serves another queue, so the job always runs on a non-leader.
	leader := startWorker(t, append(common, "-id", "leader", "-queues", "lead:1")...)
	require.True(t, waitFor(30*time.Second, 50*time.Millisecond, func() bool {
		var id string
		err := pool.QueryRow(ctx, `SELECT leader_id FROM "acc_fmn09".foreman_leader`).Scan(&id)
		return err == nil && id == leader.id
	}), "leader elected")
	addrs := map[string]string{"a": freeAddr(t), "b": freeAddr(t)}
	procs := map[string]*proc{}
	for _, id := range []string{"a", "b"} {
		procs[id] = startWorker(t, append(common, "-id", id, "-queues", "default:4", "-metrics-addr", addrs[id])...)
	}

	admin, err := foreman.NewClient(pool, foreman.Config{Schema: schema, Logger: testLogger(t)})
	require.NoError(t, err)
	// Attempt 1 takes long enough to outlive the freeze; attempt 2 is instant.
	res, err := admin.Insert(ctx, accjobs.NoopArgs{Seq: 1, FirstAttemptMS: int((bound + 15*time.Second).Milliseconds())}, nil)
	require.NoError(t, err)
	var row *foreman.JobRow
	require.True(t, waitFor(30*time.Second, 10*time.Millisecond, func() bool {
		row, err = admin.JobGet(ctx, res.Job.ID)
		return err == nil && row.State == foreman.StateRunning
	}), "job started")
	frozenID := row.AttemptedBy[0]
	otherID := map[string]string{"a": "b", "b": "a"}[frozenID]
	frozen := procs[frozenID]
	frozen.signal(syscall.SIGSTOP)
	freezeAt := time.Now()

	var final *foreman.JobRow
	completed := waitFor(bound+time.Minute, 50*time.Millisecond, func() bool {
		final, err = admin.JobGet(ctx, res.Job.ID)
		return err == nil && final.State == foreman.StateCompleted
	})
	require.True(t, completed, "job completed by another client")
	require.NotEmpty(t, final.Errors, "rescue recorded")
	rescueLatency := final.Errors[0].At.Sub(freezeAt)

	frozen.signal(syscall.SIGCONT)
	staleSeen := waitFor(bound+time.Minute, 200*time.Millisecond, func() bool {
		return scrapeMetric(t, addrs[frozenID], "foreman_stale_completions_total") >= 1
	})
	after, err := admin.JobGet(ctx, res.Job.ID)
	require.NoError(t, err)

	completedByOther := after.State == foreman.StateCompleted && len(after.AttemptedBy) == 2 &&
		after.AttemptedBy[1] == otherID && after.Attempt == 2
	measured := map[string]any{
		"rescue_latency_s": rescueLatency.Seconds(), "rescue_bound_s": bound.Seconds(),
		"frozen_client": frozenID, "completed_by": after.AttemptedBy[len(after.AttemptedBy)-1],
		"completed_by_other_client": completedByOther, "stale_completion_rejected": staleSeen,
		"final_attempt": after.Attempt, "rescue_error": final.Errors[0].Error,
	}
	pass := rescueLatency <= bound && completedByOther == th.Bool(t, "completed_by_other_client") &&
		staleSeen == th.Bool(t, "stale_completion_rejected")
	writeResult(t, "FMN-09", measured, pass, nil)
	assert.LessOrEqual(t, rescueLatency, bound, "rescued within "+strconv.Itoa(int(bound.Seconds()))+"s")
	assert.True(t, completedByOther, "completed by the other client: %+v", after.AttemptedBy)
	assert.True(t, staleSeen, "the resumed process's completion was rejected")
	assert.Equal(t, foreman.StateCompleted, after.State, "the stale completion changed nothing")
}
