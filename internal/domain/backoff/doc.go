// SPDX-License-Identifier: Apache-2.0

// Package backoff provides retry delay policies: exponential with full jitter
// (the default, base 1s, cap 24h), constant, and custom functions.
//
//	p := backoff.NewExponential()
//	next := now.Add(p.Delay(job.Attempt))
package backoff
