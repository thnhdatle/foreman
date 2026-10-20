// SPDX-License-Identifier: Apache-2.0

// Package maintenance holds the leader-only services (scheduler, rescuer,
// cleaner, reindexer, periodic enqueuer, queue-depth sampler) and the
// PollOnly cancel poller. Each service is a loop driven by the injected
// clock that records its last run and rows affected.
//
//	maintenance.RunAll(leaderCtx, &maintenance.Scheduler{Base: base}, &maintenance.Rescuer{Base: base, ...})
package maintenance
