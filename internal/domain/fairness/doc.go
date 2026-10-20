// SPDX-License-Identifier: Apache-2.0

// Package fairness decides how a fetch of k jobs is split between
// concurrency keys: by fetch order with per-key headroom, or fairly by least
// recently served key within each priority level. It is pure: the
// producer supplies the candidate keys found by a loose index scan and keeps
// the [Tracker] in memory.
//
//	tr := fairness.NewTracker()
//	allocs := fairness.Allocate(10, candidates, true, tr)
package fairness
