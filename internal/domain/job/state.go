// SPDX-License-Identifier: Apache-2.0

package job

import (
	"fmt"
	"time"
)

// State is the lifecycle state of a job.
type State string

const (
	StateAvailable State = "available"
	StateScheduled State = "scheduled"
	StateRunning   State = "running"
	StateRetryable State = "retryable"
	StateCompleted State = "completed"
	StateCancelled State = "cancelled"
	StateDiscarded State = "discarded"
)

var AllStates = []State{
	StateAvailable, StateScheduled, StateRunning, StateRetryable,
	StateCompleted, StateCancelled, StateDiscarded,
}

var PendingStates = []State{StateAvailable, StateScheduled, StateRunning, StateRetryable}

func (s State) Valid() bool { return s.Bit() >= 0 }

func (s State) IsFinal() bool {
	return s == StateCompleted || s == StateCancelled || s == StateDiscarded
}

func (s State) Bit() int {
	for i, st := range AllStates {
		if st == s {
			return i
		}
	}
	return -1
}

func ParseState(s string) (State, error) {
	st := State(s)
	if !st.Valid() {
		return "", fmt.Errorf("job: unknown state %q", s)
	}
	return st, nil
}

func InitialState(scheduledAt, now time.Time) State {
	if scheduledAt.After(now) {
		return StateScheduled
	}
	return StateAvailable
}
