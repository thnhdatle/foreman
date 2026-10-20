// SPDX-License-Identifier: Apache-2.0

//go:build acceptance

package acceptance

import (
	"context"
	"strconv"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/thnhdatle/foreman"
	"github.com/thnhdatle/foreman/acceptance/internal/accjobs"
)

// FMN-08: concurrent unique inserts from Go and SQL, in two processes, with
// int64 identities above 2^53, leave exactly one job per key.
func TestAcceptance_FMN08_Unique(t *testing.T) {
	th := loadThresholds(t, "FMN-08")
	inserts, keys := th.Int(t, "inserts"), th.Int(t, "unique_keys")
	goroutines, processes := th.Int(t, "goroutines"), th.Int(t, "processes")
	require.Equal(t, 2, processes, "this test runs one helper process next to the test process")
	require.InDelta(t, 0.5, th.Float(t, "sql_function_ratio"), 0, "inserts alternate between Go and SQL")
	ctx := context.Background()
	const schema = "acc_fmn08"
	pool := newPool(t, dbURL(), 60)
	freshSchema(t, pool, schema)
	client, err := foreman.NewClient(pool, foreman.Config{Schema: schema, Logger: testLogger(t)})
	require.NoError(t, err)
	run := time.Now().UnixNano()

	half := inserts / processes
	perGoroutine := goroutines / processes
	helper := startWorker(t, "-mode", "insert", "-database-url", dbURL(), "-schema", schema,
		"-inserts", strconv.Itoa(half), "-goroutines", strconv.Itoa(perGoroutine), "-keys", strconv.Itoa(keys),
		"-run", strconv.FormatInt(run, 10))

	start := time.Now()
	var wg sync.WaitGroup
	errs := make(chan error, perGoroutine)
	per := half / perGoroutine
	for g := 0; g < perGoroutine; g++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := 0; i < per; i++ {
				n := g*per + i
				// Offset by one so this process's Go/SQL alternation is the mirror of the helper's.
				if err := accjobs.InsertUnique(ctx, client, pool, schema, run, accjobs.BigBase+int64(n%keys), n%2 == 1); err != nil {
					errs <- err
					return
				}
			}
		}()
	}
	wg.Wait()
	close(errs)
	for err := range errs {
		require.NoError(t, err)
	}
	require.True(t, helper.wait(5*time.Minute), "helper process finished")
	require.Equal(t, 0, helper.cmd.ProcessState.ExitCode(), helper.stderr.String())
	elapsed := time.Since(start)

	var jobs, distinct int64
	require.NoError(t, pool.QueryRow(ctx, `SELECT count(*), count(DISTINCT args->>'big') FROM "acc_fmn08".foreman_jobs
		WHERE (args->>'run')::bigint = $1`, run).Scan(&jobs, &distinct))
	measured := map[string]any{
		"inserts": per*perGoroutine + half, "unique_keys": keys, "jobs": jobs, "distinct_identities": distinct,
		"goroutines": perGoroutine * processes, "processes": processes, "elapsed_s": elapsed.Seconds(),
	}
	want := int64(th.Int(t, "jobs_exact"))
	writeResult(t, "FMN-08", measured, jobs == want && distinct == want, nil)
	assert.Equal(t, want, jobs)
	assert.Equal(t, want, distinct, "ids above 2^53 differing in the last digit are distinct")
}
