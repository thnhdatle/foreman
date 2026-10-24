// SPDX-License-Identifier: Apache-2.0

package testutil

import (
	"testing"
	"time"
)

// Eventually polls cond until it returns true or timeout elapses.
func Eventually(tb testing.TB, cond func() bool, timeout time.Duration, msgAndArgs ...any) {
	tb.Helper()
	deadline := time.NewTimer(timeout)
	defer deadline.Stop()
	tick := time.NewTicker(5 * time.Millisecond)
	defer tick.Stop()
	for {
		if cond() {
			return
		}
		select {
		case <-deadline.C:
			if cond() {
				return
			}
			tb.Fatalf("condition not met within %s %v", timeout, msgAndArgs)
			return
		case <-tick.C:
		}
	}
}
