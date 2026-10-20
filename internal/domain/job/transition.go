// SPDX-License-Identifier: Apache-2.0

package job

import "fmt"

// Event is something that happens to a job and may change its state.
type Event string

// Events of the job state machine.
const (
	EventScheduleReached  Event = "schedule_reached"  // scheduler: run_at reached
	EventFetched          Event = "fetched"           // producer took the job
	EventSucceeded        Event = "succeeded"         // Work returned nil
	EventFailedRetry      Event = "failed_retry"      // error, attempts left
	EventFailedDiscard    Event = "failed_discard"    // error, no attempts left
	EventWorkerDiscard    Event = "worker_discard"    // Work returned Discard(err)
	EventWorkerCancel     Event = "worker_cancel"     // Work returned Cancel(err) or cancel was requested
	EventSnoozed          Event = "snoozed"           // Work returned Snooze(d)
	EventRescued          Event = "rescued"           // heartbeat lost, attempts left
	EventRescuedExhausted Event = "rescued_exhausted" // heartbeat lost, no attempts left
	EventBackoffElapsed   Event = "backoff_elapsed"   // scheduler: retry time reached
	EventAdminCancel      Event = "admin_cancel"      // JobCancel on a non-running job
	EventAdminDiscard     Event = "admin_discard"     // JobDiscard
	EventAdminRetry       Event = "admin_retry"       // JobRetry
)

type edge struct {
	from State
	ev   Event
}

// transitions is the complete state machine; anything absent is invalid.
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

// Next returns the state reached from `from` on event ev, or an
// *InvalidTransitionError when the state machine forbids it.
func Next(from State, ev Event) (State, error) {
	to, ok := transitions[edge{from, ev}]
	if !ok {
		return "", &InvalidTransitionError{From: from, Event: ev}
	}
	return to, nil
}

// CanApply reports whether ev is allowed in state from.
func CanApply(from State, ev Event) bool {
	_, err := Next(from, ev)
	return err == nil
}

// InvalidTransitionError reports a forbidden state change.
type InvalidTransitionError struct {
	From  State
	Event Event
}

func (e *InvalidTransitionError) Error() string {
	return fmt.Sprintf("job: %s not allowed in state %s", e.Event, e.From)
}

// Is makes errors.Is(err, ErrInvalidTransition) match.
func (e *InvalidTransitionError) Is(target error) bool { return target == ErrInvalidTransition }
