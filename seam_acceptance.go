// SPDX-License-Identifier: Apache-2.0

//go:build acceptance

package foreman

// CrashForTest simulates a crash of c (crash seam): its producers
// and leader services stop immediately, running jobs are cancelled, and the
// leader lease is abandoned without resigning, so other clients take over
// only after it expires. It exists only in builds with the acceptance tag
// and is not part of the public API. Call StopAndCancel afterwards to
// release the remaining goroutines.
func CrashForTest(c *Client) {
	c.mu.Lock()
	r := c.run
	c.mu.Unlock()
	if r != nil {
		r.crash()
	}
}
