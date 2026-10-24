// SPDX-License-Identifier: Apache-2.0

package foreman

import (
	"errors"

	"github.com/thnhdatle/foreman/internal/domain/job"
	"github.com/thnhdatle/foreman/internal/domain/unique"
)

// Errors returned by Foreman.
var (
	// ErrArgsTooLarge reports args whose JSON exceeds Config.MaxArgsBytes.
	ErrArgsTooLarge = errors.New("foreman: args too large")
	// ErrUniqueArgsNotCanonical reports a non-integral number in unique args.
	ErrUniqueArgsNotCanonical = unique.ErrArgsNotCanonical
	// ErrInvalidConfig wraps every problem found by NewClient.
	ErrInvalidConfig = errors.New("foreman: invalid config")
	// ErrInvalidInsertOpts wraps every problem with InsertOpts or UniqueOpts.
	ErrInvalidInsertOpts = errors.New("foreman: invalid insert options")
	// ErrInvalidArgument reports a nil or malformed argument.
	ErrInvalidArgument = errors.New("foreman: invalid argument")
	// ErrNotFound reports a job id that does not exist.
	ErrNotFound = job.ErrNotFound
	// ErrInvalidTransition reports an admin operation not allowed in the job's state.
	ErrInvalidTransition = job.ErrInvalidTransition
	// ErrUniqueConflict reports a retry that would duplicate a unique job.
	ErrUniqueConflict = job.ErrUniqueConflict
	// ErrStaleJob reports that the attempt is no longer owned by this client.
	ErrStaleJob = job.ErrStale
	// ErrNotInWorker reports a call that is only valid inside Worker.Work.
	ErrNotInWorker = errors.New("foreman: not called from inside Work")
	// ErrAlreadyStarted reports a second Start of a running client.
	ErrAlreadyStarted = errors.New("foreman: client already started")
	// ErrNotStarted reports Stop of a client that is not running.
	ErrNotStarted = errors.New("foreman: client not started")
	// ErrSchemaNotMigrated reports a database without the required Foreman schema.
	ErrSchemaNotMigrated = errors.New("foreman: schema not migrated")
	// ErrNoWorker reports a kind without a registered worker.
	ErrNoWorker = job.ErrNoWorker
	// ErrClientStopping is the cause of a job's context when the client cancels it on shutdown.
	ErrClientStopping = job.ErrClientStopping
)
