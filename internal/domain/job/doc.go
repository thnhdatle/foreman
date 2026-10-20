// SPDX-License-Identifier: Apache-2.0

// Package job defines the job row, its state machine and the mapping from
// the result of Work to the state update written to the database.
//
// Main types: [State], [Event], [Row], [Completion], [Outcome].
//
//	next, err := job.Next(job.StateRunning, job.EventSucceeded) // completed
//	c := job.MapResult(row, job.Outcome{Err: err}, now, retryAt)
package job
