// SPDX-License-Identifier: Apache-2.0

package job

import (
	"fmt"
	"time"
)

// State is the lifecycle state of a job row.
type State string

// Job states. The order of AllStates defines the bit position of each state in
// the unique_states bit(8) mask (position 0 is the leftmost bit).
const (
	StateAvailable State = "available"
	StateScheduled State = "scheduled"
	StateRunning   State = "running"
	StateRetryable State = "retryable"
	StateCompleted State = "completed"
	StateCancelled State = "cancelled"
	StateDiscarded State = "discarded"
)

// AllStates lists every state in bit order.
var AllStates = []State{
	StateAvailable, StateScheduled, StateRunning, StateRetryable,
	StateCompleted, StateCancelled, StateDiscarded,
}

// PendingStates are the states a job passes through before it is finalized.
var PendingStates = []State{StateAvailable, StateScheduled, StateRunning, StateRetryable}

// Valid reports whether s is a known state.
func (s State) Valid() bool { return s.Bit() >= 0 }

// IsFinal reports whether s is completed, cancelled or discarded.
func (s State) IsFinal() bool {
	return s == StateCompleted || s == StateCancelled || s == StateDiscarded
}

// Bit returns the position of s in the unique_states mask, or -1 if s is unknown.
func (s State) Bit() int {
	for i, st := range AllStates {
		if st == s {
			return i
		}
	}
	return -1
}

// ParseState converts a string into a State.
func ParseState(s string) (State, error) {
	st := State(s)
	if !st.Valid() {
		return "", fmt.Errorf("job: unknown state %q", s)
	}
	return st, nil
}

// InitialState returns the state of a newly inserted job: scheduled when its
// scheduled time is in the future, available otherwise.
func InitialState(scheduledAt, now time.Time) State {
	if scheduledAt.After(now) {
		return StateScheduled
	}
	return StateAvailable
}
