// SPDX-License-Identifier: Apache-2.0

// Package pgstore holds every SQL statement of Foreman (pgx v5): inserts
// (multi-row and COPY), the fetch queries with SKIP LOCKED, fairness and
// per-key limits, batched completions and heartbeats under the completion
// guard, leases, maintenance, admin operations and the LISTEN connection.
// Time values always come from the caller's clock.
//
//	store, err := pgstore.New(pool, "public")
//	rows, err := store.Fetch(ctx, job.FetchParams{Queue: "default", ClientID: id, Limit: 10, Now: now})
package pgstore
