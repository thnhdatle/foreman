// SPDX-License-Identifier: Apache-2.0

package foreman

import (
	"encoding/json"
	"time"
)

// Default insert values.
const (
	DefaultQueue       = "default"
	DefaultPriority    = 2
	DefaultMaxAttempts = 25
)

// InsertOpts are per-job options.
type InsertOpts struct {
	// Queue defaults to "default".
	Queue string
	// Priority is 1 to 4, where 1 runs first. Default 2.
	Priority int
	// ScheduledAt in the future schedules the job.
	ScheduledAt time.Time
	// MaxAttempts defaults to 25.
	MaxAttempts int
	Tags        []string
	// Metadata must be a JSON object.
	Metadata json.RawMessage
	// ConcurrencyKey groups jobs for per-key limits and fairness (for example a tenant id).
	ConcurrencyKey string
	UniqueOpts     *UniqueOpts
	Timeout        time.Duration
}

// UniqueOpts selects the identity of a unique job.
type UniqueOpts struct {
	ByArgs      bool
	ByArgFields []string
	ByPeriod    time.Duration
	ByQueue     bool
	// ByState defaults to available, scheduled, running, retryable and completed.
	ByState []State
}

// InsertParams is one job of an InsertMany call.
type InsertParams struct {
	Args       JobArgs
	InsertOpts *InsertOpts
}

// InsertResult is the outcome of inserting one job.
type InsertResult struct {
	Job                      *JobRow
	UniqueSkippedAsDuplicate bool
}

// QueueConfig configures one queue served by a client.
type QueueConfig struct {
	// MaxWorkers is how many jobs run at once. Default 10.
	MaxWorkers int
	// FetchInterval is the polling interval. Default 1s.
	FetchInterval time.Duration
	// PerKeyLimit caps running jobs per concurrency key. 0 means no limit.
	PerKeyLimit int
	// Fairness alternates between concurrency keys when jobs are backlogged.
	Fairness bool
}
