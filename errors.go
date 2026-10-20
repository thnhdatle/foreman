// SPDX-License-Identifier: Apache-2.0

package foreman

import (
	"errors"

	"github.com/thnhdatle/foreman/internal/domain/job"
	"github.com/thnhdatle/foreman/internal/domain/unique"
)

// Errors returned by Foreman. They are listed with their meaning in
// docs/guides/errors.md and are part of the public API.
var (
	// ErrArgsTooLarge reports args whose JSON exceeds Config.MaxArgsBytes.
	ErrArgsTooLarge = errors.New("foreman: args too large")
	// ErrUniqueArgsNotCanonical reports a non-integral number in the
	// identity of a unique job; use strings for decimals in unique fields.
	ErrUniqueArgsNotCanonical = unique.ErrArgsNotCanonical
	// ErrInvalidConfig wraps every problem found by NewClient.
	ErrInvalidConfig = errors.New("foreman: invalid config")
	// ErrInvalidInsertOpts wraps every problem with InsertOpts or UniqueOpts.
	ErrInvalidInsertOpts = errors.New("foreman: invalid insert options")
	// ErrInvalidArgument reports a nil or malformed argument.
	ErrInvalidArgument = errors.New("foreman: invalid argument")
	// ErrNotFound reports a job id that does not exist.
	ErrNotFound = job.ErrNotFound
	// ErrInvalidTransition reports an admin operation not allowed in the
	// job's current state (for example retrying a running job).
	ErrInvalidTransition = job.ErrInvalidTransition
	// ErrUniqueConflict reports a retry that would duplicate a unique job.
	ErrUniqueConflict = job.ErrUniqueConflict
	// ErrStaleJob reports that CompleteTx found the attempt no longer owned
	// by this client (it was rescued, cancelled or discarded).
	ErrStaleJob = job.ErrStale
	// ErrNotInWorker reports a call that is only valid inside Worker.Work.
	ErrNotInWorker = errors.New("foreman: not called from inside Work")
	// ErrAlreadyStarted reports a second Start of a running client.
	ErrAlreadyStarted = errors.New("foreman: client already started")
	// ErrNotStarted reports Stop of a client that is not running.
	ErrNotStarted = errors.New("foreman: client not started")
	// ErrSchemaNotMigrated reports a database without the Foreman schema at
	// the version this module needs; run the migrations first.
	ErrSchemaNotMigrated = errors.New("foreman: schema not migrated")
	// ErrNoWorker reports a kind without a registered worker: the error of
	// attempts of such jobs (they are retried) and of JobWork.
	ErrNoWorker = job.ErrNoWorker
	// ErrClientStopping is the context.Cause of a job's context when the
	// client cancels it on shutdown (StopAndCancel or a Stop deadline);
	// operator cancels use a different cause and end the job as cancelled.
	ErrClientStopping = job.ErrClientStopping
)
