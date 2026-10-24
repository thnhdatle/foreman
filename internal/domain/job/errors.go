// SPDX-License-Identifier: Apache-2.0

package job

import (
	"errors"
	"fmt"
	"time"
)

var (
	ErrInvalidTransition = errors.New("job: invalid state transition")
	ErrNotFound          = errors.New("job: not found")
	ErrStale             = errors.New("job: stale attempt")
	ErrUniqueConflict    = errors.New("job: a unique job with the same key already exists")
)

type SnoozeError struct{ Duration time.Duration }

func (e *SnoozeError) Error() string { return fmt.Sprintf("job snoozed for %s", e.Duration) }

type DiscardError struct{ Err error }

func (e *DiscardError) Error() string { return "job discarded: " + errString(e.Err) }

func (e *DiscardError) Unwrap() error { return e.Err }

type CancelError struct{ Err error }

func (e *CancelError) Error() string { return "job cancelled: " + errString(e.Err) }

func (e *CancelError) Unwrap() error { return e.Err }

var (
	ErrCancelRequested = errors.New("job: cancel requested")
	ErrClientStopping  = errors.New("job: client stopping")
	ErrNoWorker        = errors.New("foreman: no worker registered for kind")
)

func errString(err error) string {
	if err == nil {
		return "no reason given"
	}
	return err.Error()
}
