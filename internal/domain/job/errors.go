// SPDX-License-Identifier: Apache-2.0

package job

import (
	"errors"
	"fmt"
	"time"
)

// Domain errors.
var (
	// ErrInvalidTransition is matched by every *InvalidTransitionError.
	ErrInvalidTransition = errors.New("job: invalid state transition")
	// ErrNotFound reports a job id that does not exist.
	ErrNotFound = errors.New("job: not found")
	// ErrStale reports a completion rejected by the completion guard.
	ErrStale = errors.New("job: stale attempt")
	// ErrUniqueConflict reports an admin retry that would duplicate a unique job.
	ErrUniqueConflict = errors.New("job: a unique job with the same key already exists")
)

// SnoozeError is returned by Work (via foreman.Snooze) to run the job again
// after Duration without consuming an attempt.
type SnoozeError struct{ Duration time.Duration }

func (e *SnoozeError) Error() string { return fmt.Sprintf("job snoozed for %s", e.Duration) }

// DiscardError is returned by Work (via foreman.Discard) to discard the job
// immediately, whatever attempts remain.
type DiscardError struct{ Err error }

func (e *DiscardError) Error() string { return "job discarded: " + errString(e.Err) }

// Unwrap returns the reason.
func (e *DiscardError) Unwrap() error { return e.Err }

// CancelError is returned by Work (via foreman.Cancel) to cancel the job.
type CancelError struct{ Err error }

func (e *CancelError) Error() string { return "job cancelled: " + errString(e.Err) }

// Unwrap returns the reason.
func (e *CancelError) Unwrap() error { return e.Err }

// Context cancellation causes and execution errors shared by app and API.
var (
	// ErrCancelRequested is the cause of a job context cancelled by an operator.
	ErrCancelRequested = errors.New("job: cancel requested")
	// ErrClientStopping is the cause of a job context cancelled by StopAndCancel
	// or by a Stop deadline.
	ErrClientStopping = errors.New("job: client stopping")
	// ErrNoWorker reports a job whose kind has no registered worker.
	ErrNoWorker = errors.New("foreman: no worker registered for kind")
)

func errString(err error) string {
	if err == nil {
		return "no reason given"
	}
	return err.Error()
}
