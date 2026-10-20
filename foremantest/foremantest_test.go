// SPDX-License-Identifier: Apache-2.0

package foremantest_test

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"

	"github.com/thnhdatle/foreman"
	"github.com/thnhdatle/foreman/foremantest"
)

type mailArgs struct {
	To string `json:"to"`
}

func (mailArgs) Kind() string { return "mail" }

func TestWorkJob_Outcomes(t *testing.T) {
	t.Parallel()
	clk := foremantest.NewClock(time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC))
	tests := []struct {
		name  string
		work  func(context.Context, *foreman.Job[mailArgs]) error
		opts  *foremantest.WorkOpts
		state foreman.State
		err   string
	}{
		{name: "success", work: func(_ context.Context, j *foreman.Job[mailArgs]) error {
			if j.Args.To != "a@example.com" {
				return errors.New("wrong args")
			}
			return nil
		}, state: foreman.StateCompleted},
		{
			name: "error retries", work: func(context.Context, *foreman.Job[mailArgs]) error { return errors.New("smtp down") },
			state: foreman.StateRetryable, err: "smtp down",
		},
		{
			name: "last attempt discards", opts: &foremantest.WorkOpts{Attempt: 3, MaxAttempts: 3, Clock: clk},
			work:  func(context.Context, *foreman.Job[mailArgs]) error { return errors.New("x") },
			state: foreman.StateDiscarded, err: "x",
		},
		{
			name: "snooze", work: func(context.Context, *foreman.Job[mailArgs]) error { return foreman.Snooze(time.Minute) },
			state: foreman.StateScheduled,
		},
		{
			name: "panic", work: func(context.Context, *foreman.Job[mailArgs]) error { panic("oops") },
			state: foreman.StateRetryable, err: "panic: oops",
		},
		{
			name: "timeout", opts: &foremantest.WorkOpts{Timeout: 10 * time.Millisecond, RetryPolicy: foreman.ConstantBackoff{}},
			work: func(ctx context.Context, _ *foreman.Job[mailArgs]) error {
				<-ctx.Done()
				return ctx.Err()
			}, state: foreman.StateRetryable, err: "deadline exceeded",
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			res := foremantest.WorkJob(context.Background(), t, foreman.WorkFunc[mailArgs](tc.work),
				mailArgs{To: "a@example.com"}, tc.opts)
			assert.Equal(t, tc.state, res.State)
			assert.Contains(t, res.Err, tc.err)
		})
	}
}

func TestClock(t *testing.T) {
	t.Parallel()
	start := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	c := foremantest.NewClock(start)
	var _ foreman.Clock = c
	ch := c.After(time.Second)
	tk := c.NewTicker(time.Second)
	assert.Equal(t, 2, c.Waiters())
	c.Advance(time.Second)
	<-ch
	<-tk.C()
	tk.Stop()
	c.Set(start.Add(time.Hour))
	assert.Equal(t, start.Add(time.Hour), c.Now())
}
