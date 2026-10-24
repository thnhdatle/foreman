// SPDX-License-Identifier: Apache-2.0

package foremantest_test

import (
	"context"
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/thnhdatle/foreman"
	"github.com/thnhdatle/foreman/foremantest"
)

func ExampleWorkJob() {
	worker := foreman.WorkFunc[mailArgs](func(context.Context, *foreman.Job[mailArgs]) error {
		return errors.New("smtp unavailable")
	})
	res := foremantest.WorkJob(context.Background(), &testing.T{}, worker, mailArgs{To: "a@example.com"}, nil)
	fmt.Println(res.State, res.Err)
	// Output: retryable smtp unavailable
}

func ExampleWorkInline() {
	var (
		t      *testing.T
		client *foreman.Client
	)
	row := foremantest.WorkInline(context.Background(), t, client, mailArgs{To: "a@example.com"}, nil)
	fmt.Println(row.State)
}

func ExampleRequireInserted() {
	var (
		t      *testing.T
		client *foreman.Client
	)
	row := foremantest.RequireInserted(context.Background(), t, client, mailArgs{To: "a@example.com"})
	fmt.Println(row.Queue)
}

func ExampleRequireNotInserted() {
	var (
		t      *testing.T
		client *foreman.Client
	)
	foremantest.RequireNotInserted(context.Background(), t, client, mailArgs{To: "blocked@example.com"})
}

func ExampleNewClock() {
	clk := foremantest.NewClock(time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC))
	clk.Advance(90 * time.Minute)
	fmt.Println(clk.Now())
	// Output: 2026-01-01 01:30:00 +0000 UTC
}
