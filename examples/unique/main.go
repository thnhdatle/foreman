// SPDX-License-Identifier: Apache-2.0

// Command unique inserts the same unique job twice; the second insert
// returns the first job instead of creating a duplicate.
package main

import (
	"context"
	"fmt"
	"log"
	"os"
	"time"

	"github.com/thnhdatle/foreman"
	"github.com/thnhdatle/foreman/examples/internal/exampledb"
)

// SyncArgs identify an account to synchronise.
type SyncArgs struct {
	AccountID int64  `json:"account_id"`
	Reason    string `json:"reason"`
}

// Kind identifies the job type.
func (SyncArgs) Kind() string { return "sync_account" }

func main() {
	if err := run(context.Background()); err != nil {
		log.Println(err)
		os.Exit(1)
	}
}

func run(ctx context.Context) error {
	pool, err := exampledb.Open(ctx, "example_unique")
	if err != nil {
		return err
	}
	defer pool.Close()
	client, err := foreman.NewClient(pool, foreman.Config{Schema: "example_unique"})
	if err != nil {
		return err
	}
	// One sync per account per hour, whatever the reason.
	opts := &foreman.InsertOpts{UniqueOpts: &foreman.UniqueOpts{
		ByArgFields: []string{"account_id"},
		ByPeriod:    time.Hour,
	}}
	first, err := client.Insert(ctx, SyncArgs{AccountID: 9007199254740993, Reason: "webhook"}, opts)
	if err != nil {
		return err
	}
	second, err := client.Insert(ctx, SyncArgs{AccountID: 9007199254740993, Reason: "manual"}, opts)
	if err != nil {
		return err
	}
	other, err := client.Insert(ctx, SyncArgs{AccountID: 9007199254740992, Reason: "manual"}, opts)
	if err != nil {
		return err
	}
	fmt.Printf("first insert: job %d\n", first.Job.ID)
	fmt.Printf("second insert: duplicate skipped: %t (job %d)\n", second.UniqueSkippedAsDuplicate, second.Job.ID)
	fmt.Printf("another account: duplicate skipped: %t\n", other.UniqueSkippedAsDuplicate)
	return nil
}
