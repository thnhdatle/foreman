// SPDX-License-Identifier: Apache-2.0

// Package foreman is a Postgres-backed job queue for Go: enqueue jobs in the
// same transaction as your data, run them with retries, cron, unique jobs and
// fair per-tenant concurrency, with no broker to operate.
//
// Main types: [Client] (inserts jobs, runs worker pools and maintenance),
// [Worker] (typed job handler), [JobArgs] (job payload), [Config].
//
//	type EmailArgs struct {
//		UserID int64 `json:"user_id"`
//	}
//
//	func (EmailArgs) Kind() string { return "email" }
//
//	workers := foreman.NewWorkers()
//	foreman.AddWorker(workers, &EmailWorker{})
//	client, err := foreman.NewClient(pool, foreman.Config{
//		Queues:  map[string]foreman.QueueConfig{"default": {MaxWorkers: 50}},
//		Workers: workers,
//	})
package foreman
