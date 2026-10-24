// SPDX-License-Identifier: Apache-2.0

package job

import "fmt"

// Event is something that happens to a job and may change its state.
type Event string

const (
	EventScheduleReached  Event = "schedule_reached"
	EventFetched          Event = "fetched"
	EventSucceeded        Event = "succeeded"
	EventFailedRetry      Event = "failed_retry"
	EventFailedDiscard    Event = "failed_discard"
	EventWorkerDiscard    Event = "worker_discard"
	EventWorkerCancel     Event = "worker_cancel"
	EventSnoozed          Event = "snoozed"
	EventRescued          Event = "rescued"
	EventRescuedExhausted Event = "rescued_exhausted"
	EventBackoffElapsed   Event = "backoff_elapsed"
	EventAdminCancel      Event = "admin_cancel"
	EventAdminDiscard     Event = "admin_discard"
	EventAdminRetry       Event = "admin_retry"
)

type edge struct {
	from State
	ev   Event
}

var transitions = map[edge]State{
	{StateScheduled, EventScheduleReached}: StateAvailable,
	{StateAvailable, EventFetched}:         StateRunning,
	{StateRunning, EventSucceeded}:         StateCompleted,
	{StateRunning, EventFailedRetry}:       StateRetryable,
	{StateRunning, EventRescued}:           StateRetryable,
	{StateRunning, EventFailedDiscard}:     StateDiscarded,
	{StateRunning, EventWorkerDiscard}:     StateDiscarded,
	{StateRunning, EventRescuedExhausted}:  StateDiscarded,
	{StateRunning, EventWorkerCancel}:      StateCancelled,
	{StateRunning, EventSnoozed}:           StateScheduled,
	{StateRetryable, EventBackoffElapsed}:  StateAvailable,
	{StateAvailable, EventAdminCancel}:     StateCancelled,
	{StateScheduled, EventAdminCancel}:     StateCancelled,
	{StateRetryable, EventAdminCancel}:     StateCancelled,
	{StateAvailable, EventAdminDiscard}:    StateDiscarded,
	{StateScheduled, EventAdminDiscard}:    StateDiscarded,
	{StateRetryable, EventAdminDiscard}:    StateDiscarded,
	{StateRunning, EventAdminDiscard}:      StateDiscarded,
	{StateDiscarded, EventAdminRetry}:      StateAvailable,
	{StateCancelled, EventAdminRetry}:      StateAvailable,
	{StateCompleted, EventAdminRetry}:      StateAvailable,
}

// Next returns the state reached from a state on an event, or an error when the transition is not allowed.
func Next(from State, ev Event) (State, error) {
	to, ok := transitions[edge{from, ev}]
	if !ok {
		return "", &InvalidTransitionError{From: from, Event: ev}
	}
	return to, nil
}

func CanApply(from State, ev Event) bool {
	_, err := Next(from, ev)
	return err == nil
}

type InvalidTransitionError struct {
	From  State
	Event Event
}

func (e *InvalidTransitionError) Error() string {
	return fmt.Sprintf("job: %s not allowed in state %s", e.Event, e.From)
}

func (e *InvalidTransitionError) Is(target error) bool { return target == ErrInvalidTransition }
