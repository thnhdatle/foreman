// SPDX-License-Identifier: Apache-2.0

package job

import "time"

// Row is a job as stored in foreman_jobs.
type Row struct {
	ID                int64
	Kind              string
	Queue             string
	Args              []byte
	State             State
	Priority          int
	Attempt           int
	MaxAttempts       int
	ScheduledAt       time.Time
	CreatedAt         time.Time
	AttemptedAt       *time.Time
	FinalizedAt       *time.Time
	HeartbeatAt       *time.Time
	CancelRequestedAt *time.Time
	AttemptedBy       []string
	Errors            []AttemptError
	Tags              []string
	Metadata          []byte
	UniqueKey         []byte
	UniqueStates      []State
	ConcurrencyKey    string // empty means the job has no concurrency key
	Timeout           time.Duration
}

// AttemptError records one failed attempt; stored as an element of the
// errors jsonb[] column.
type AttemptError struct {
	At      time.Time `json:"at"`
	Attempt int       `json:"attempt"`
	Error   string    `json:"error"`
	Trace   string    `json:"trace,omitempty"`
}

// InsertParams is a fully resolved job ready to be inserted.
type InsertParams struct {
	Kind        string
	Queue       string
	Args        []byte
	State       State
	Priority    int
	MaxAttempts int
	ScheduledAt time.Time
	CreatedAt   time.Time
	Tags        []string
	Metadata    []byte
	// UniqueIdentity is the identity object whose canonical form is hashed in
	// SQL into unique_key; nil for non-unique jobs.
	UniqueIdentity []byte
	// UniqueStates is the bit(8) literal of states covered by the unique key.
	UniqueStates   string
	ConcurrencyKey string
	Timeout        time.Duration
}

// InsertResult is the outcome of inserting one job.
type InsertResult struct {
	Row *Row
	// Skipped is true when a unique job already existed; Row is that job.
	Skipped bool
}

// Ref identifies one attempt of a job held by a client (completion guard).
type Ref struct {
	ID      int64
	Attempt int
}

// Completion is the state update the client applies after an attempt.
type Completion struct {
	ID      int64
	Attempt int // attempt that ran; part of the completion guard
	State   State
	// At is when the result was mapped; it finalizes a job whose cancel was
	// requested after Work returned (the store then forces cancelled).
	At time.Time
	// ScheduledAt is the next run time for retryable and scheduled outcomes.
	ScheduledAt time.Time
	// FinalizedAt is set for completed, cancelled and discarded outcomes.
	FinalizedAt time.Time
	Error       *AttemptError
	// AttemptDelta is added to attempt: -1 for a snooze, which does not consume an attempt.
	AttemptDelta int
	// Kind and Queue are carried for metrics; they are not written.
	Kind  string
	Queue string
}
