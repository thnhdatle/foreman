// SPDX-License-Identifier: Apache-2.0

package metrics

// Histogram buckets, declared once for the project.
var (
	// DurationBuckets covers job run times from 1 ms to ~1 h.
	DurationBuckets = []float64{
		0.001, 0.0025, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1, 2.5, 5, 10, 30, 60, 300, 900, 3600,
	}
	// LatencyBuckets covers queue latency (scheduled_at to attempted_at) from 1 ms to 1 h.
	LatencyBuckets = []float64{
		0.001, 0.0025, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1, 2.5, 5, 10, 30, 60, 300, 3600,
	}
	// FetchBuckets covers fetch statements from 100 µs to 5 s.
	FetchBuckets = []float64{0.0001, 0.00025, 0.0005, 0.001, 0.0025, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 1, 5}
)
