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

// InsertOpts are per-job options. Zero fields fall back to the args type's
// InsertOpts(), then to the worker options, then to the defaults.
type InsertOpts struct {
	// Queue defaults to "default".
	Queue string
	// Priority is 1 (highest) to 4 (lowest); default 2.
	Priority int
	// ScheduledAt in the future inserts the job as scheduled.
	ScheduledAt time.Time
	// MaxAttempts defaults to 25.
	MaxAttempts int
	Tags        []string
	// Metadata must be a JSON object.
	Metadata json.RawMessage
	// ConcurrencyKey groups jobs for per-key limits and fairness (for example a tenant id).
	ConcurrencyKey string
	// UniqueOpts makes the job unique; nil means not unique.
	UniqueOpts *UniqueOpts
	// Timeout overrides the kind and client job timeout.
	Timeout time.Duration
}

// UniqueOpts selects the identity of a unique job: its kind plus, optionally,
// its args (all, or selected top-level fields), its queue and a time period.
// While a job with the same identity exists in one of ByState, inserting
// another returns the existing job with UniqueSkippedAsDuplicate set.
type UniqueOpts struct {
	// ByArgs includes all args; mutually exclusive with ByArgFields.
	ByArgs bool
	// ByArgFields includes only these top-level args fields.
	ByArgFields []string
	// ByPeriod includes the start of the current period (whole seconds).
	ByPeriod time.Duration
	// ByQueue includes the queue.
	ByQueue bool
	// ByState defaults to available, scheduled, running, retryable and
	// completed; it must include the first four.
	ByState []State
}

// InsertParams is one job of an InsertMany call.
type InsertParams struct {
	Args       JobArgs
	InsertOpts *InsertOpts
}

// InsertResult is the outcome of inserting one job.
type InsertResult struct {
	// Job is the inserted job, or the existing job for a skipped duplicate.
	Job *JobRow
	// UniqueSkippedAsDuplicate is true when a unique job already existed.
	UniqueSkippedAsDuplicate bool
}

// QueueConfig configures one queue served by a client.
type QueueConfig struct {
	// MaxWorkers is the number of jobs run at once; default 10.
	MaxWorkers int
	// FetchInterval is the polling fallback; default 1s.
	FetchInterval time.Duration
	// PerKeyLimit caps running jobs per concurrency key across the cluster; 0 = no limit.
	PerKeyLimit int
	// Fairness alternates between concurrency keys when jobs are backlogged.
	Fairness bool
}
