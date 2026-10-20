// SPDX-License-Identifier: Apache-2.0

// Package schedule wraps the cron parser, provides epoch-aligned Every
// schedules, and computes which periodic ticks are due after a leader gap
// (catch-up window) and which are skipped.
//
//	s, err := schedule.ParseCron("*/5 * * * *", loc)
//	due, skipped := schedule.DueTicks(s, lastTick, now, 5*time.Minute, 100)
package schedule
