// SPDX-License-Identifier: Apache-2.0

package job

import "time"

// Row is a job as stored in the database.
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
	ConcurrencyKey    string
	Timeout           time.Duration
}

type AttemptError struct {
	At      time.Time `json:"at"`
	Attempt int       `json:"attempt"`
	Error   string    `json:"error"`
	Trace   string    `json:"trace,omitempty"`
}

type InsertParams struct {
	Kind           string
	Queue          string
	Args           []byte
	State          State
	Priority       int
	MaxAttempts    int
	ScheduledAt    time.Time
	CreatedAt      time.Time
	Tags           []string
	Metadata       []byte
	UniqueIdentity []byte
	UniqueStates   string
	ConcurrencyKey string
	Timeout        time.Duration
}

type InsertResult struct {
	Row     *Row
	Skipped bool
}

type Ref struct {
	ID      int64
	Attempt int
}

// Completion is the state update written after an attempt.
type Completion struct {
	ID           int64
	Attempt      int
	State        State
	At           time.Time
	ScheduledAt  time.Time
	FinalizedAt  time.Time
	Error        *AttemptError
	AttemptDelta int
	Kind         string
	Queue        string
}
