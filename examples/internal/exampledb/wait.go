// SPDX-License-Identifier: Apache-2.0

package exampledb

import (
	"context"
	"fmt"
	"time"

	"github.com/thnhdatle/foreman"
)

func WaitState(ctx context.Context, c *foreman.Client, id int64, state foreman.State, timeout time.Duration) (
	*foreman.JobRow, error,
) {
	deadline := time.Now().Add(timeout)
	for {
		job, err := c.JobGet(ctx, id)
		if err != nil {
			return nil, err //nolint:wrapcheck // examples print errors as is
		}
		if job.State == state {
			return job, nil
		}
		if time.Now().After(deadline) {
			return job, fmt.Errorf("job %d is %s after %s, want %s", id, job.State, timeout, state)
		}
		<-time.After(20 * time.Millisecond)
	}
}
