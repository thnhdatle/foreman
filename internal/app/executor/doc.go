// SPDX-License-Identifier: Apache-2.0

// Package executor runs one attempt of a job: it applies the job, kind or
// client timeout, recovers panics with their stack, consults the error
// handler, honours cancel requests, and maps the result to a completion.
//
//	ex, err := executor.New(executor.Config{Lookup: lookup, DefaultRetry: retry, Clock: clk})
//	completion, skip := ex.Execute(jobCtx, row)
package executor
