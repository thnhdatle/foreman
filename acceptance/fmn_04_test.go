// SPDX-License-Identifier: Apache-2.0

//go:build acceptance

package acceptance

import (
	"fmt"
	"testing"
	"time"

	"github.com/thnhdatle/foreman"
)

// FMN-04: throughput of no-op jobs with 4 clients x 50 workers on a queue
// pre-filled with 1M jobs, sustained for 5 minutes; a second run with
// fairness and a per-key limit over 1,000 keys is reported, not gated.
func BenchmarkAcceptance_FMN04_Throughput(b *testing.B) {
	th := loadThresholds(b, "FMN-04")
	prefillN, clients, workers := th.Int(b, "prefill_jobs"), th.Int(b, "clients"), th.Int(b, "workers_per_client")
	d := time.Duration(th.Int(b, "duration_s")) * time.Second
	keys, limit := th.Int(b, "fair_keys"), th.Int(b, "fair_per_key_limit")
	pool := newPool(b, dbURL(), int32(clients*25))
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		plain := runThroughput(b, pool, "acc_fmn04", clients, workers,
			foreman.QueueConfig{FetchInterval: 100 * time.Millisecond}, prefillN, d, func(int) string { return "" })
		fair := runThroughput(b, pool, "acc_fmn04", clients, workers,
			foreman.QueueConfig{FetchInterval: 100 * time.Millisecond, Fairness: true, PerKeyLimit: limit},
			prefillN, d, func(i int) string { return fmt.Sprintf("key-%d", i%keys) })
		b.ReportMetric(plain.jobsPerSec, "jobs/s")
		b.ReportMetric(fair.jobsPerSec, "fair_jobs/s")
		measured := map[string]any{
			"throughput_jobs_per_s": plain.jobsPerSec, "completed": plain.completed, "duration_s": d.Seconds(),
			"top_ups": plain.topUps, "fair_throughput_jobs_per_s": fair.jobsPerSec, "fair_completed": fair.completed,
			"fair_keys": keys, "fair_per_key_limit": limit,
		}
		pass := plain.jobsPerSec >= th.Float(b, "throughput_min_jobs_per_s")
		writeResult(b, "FMN-04", measured, pass, nil)
		if !pass {
			b.Fatalf("throughput %.0f jobs/s below %.0f", plain.jobsPerSec, th.Float(b, "throughput_min_jobs_per_s"))
		}
	}
}
