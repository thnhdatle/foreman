// SPDX-License-Identifier: Apache-2.0

package foreman

import (
	"encoding/json"
	"fmt"
	"time"

	"github.com/thnhdatle/foreman/internal/domain/job"
)

// JobArgs is the payload of a job.
type JobArgs interface {
	Kind() string
}

// JobArgsWithInsertOpts is implemented by args types that provide insert defaults.
type JobArgsWithInsertOpts interface {
	JobArgs
	InsertOpts() InsertOpts
}

// State is the lifecycle state of a job.
type State string

// Job states.
const (
	StateAvailable State = "available"
	StateScheduled State = "scheduled"
	StateRunning   State = "running"
	StateRetryable State = "retryable"
	StateCompleted State = "completed"
	StateCancelled State = "cancelled"
	StateDiscarded State = "discarded"
)

// AttemptError records one failed attempt of a job.
type AttemptError struct {
	At      time.Time `json:"at"`
	Attempt int       `json:"attempt"`
	Error   string    `json:"error"`
	Trace   string    `json:"trace,omitempty"`
}

// Job is a job with decoded, typed args, as passed to Worker.Work.
type Job[T JobArgs] struct {
	ID          int64
	Args        T
	Attempt     int
	MaxAttempts int
	Queue       string
	Priority    int
	Tags        []string
	Metadata    json.RawMessage
	CreatedAt   time.Time
	ScheduledAt time.Time
	Errors      []AttemptError
}

// JobRow is a job as stored in the database, with undecoded args.
type JobRow struct {
	ID                int64
	Kind              string
	Queue             string
	Args              json.RawMessage
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
	Metadata          json.RawMessage
	UniqueKey         []byte
	UniqueStates      []State
	ConcurrencyKey    string
	Timeout           time.Duration
}

// DecodeArgs decodes the args of row into T. It fails if row is of another kind.
func DecodeArgs[T JobArgs](row *JobRow) (T, error) {
	var args T
	if row == nil {
		return args, fmt.Errorf("%w: nil job row", ErrInvalidArgument)
	}
	if want := args.Kind(); row.Kind != want {
		return args, fmt.Errorf("%w: job %d is of kind %q, not %q", ErrInvalidArgument, row.ID, row.Kind, want)
	}
	if err := json.Unmarshal(row.Args, &args); err != nil {
		return args, fmt.Errorf("foreman: decode args of job %d: %w", row.ID, err)
	}
	return args, nil
}

func toJobRow(r *job.Row) *JobRow {
	if r == nil {
		return nil
	}
	states := make([]State, len(r.UniqueStates))
	for i, s := range r.UniqueStates {
		states[i] = State(s)
	}
	return &JobRow{
		ID: r.ID, Kind: r.Kind, Queue: r.Queue, Args: r.Args, State: State(r.State), Priority: r.Priority,
		Attempt: r.Attempt, MaxAttempts: r.MaxAttempts, ScheduledAt: r.ScheduledAt, CreatedAt: r.CreatedAt,
		AttemptedAt: r.AttemptedAt, FinalizedAt: r.FinalizedAt, HeartbeatAt: r.HeartbeatAt,
		CancelRequestedAt: r.CancelRequestedAt, AttemptedBy: r.AttemptedBy, Errors: toAttemptErrors(r.Errors),
		Tags: r.Tags, Metadata: r.Metadata, UniqueKey: r.UniqueKey, UniqueStates: states,
		ConcurrencyKey: r.ConcurrencyKey, Timeout: r.Timeout,
	}
}

func toAttemptErrors(es []job.AttemptError) []AttemptError {
	if len(es) == 0 {
		return nil
	}
	out := make([]AttemptError, len(es))
	for i, e := range es {
		out[i] = AttemptError{At: e.At, Attempt: e.Attempt, Error: e.Error, Trace: e.Trace}
	}
	return out
}

func toJobRows(rs []*job.Row) []*JobRow {
	out := make([]*JobRow, len(rs))
	for i, r := range rs {
		out[i] = toJobRow(r)
	}
	return out
}
