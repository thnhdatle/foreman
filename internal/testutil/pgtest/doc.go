// SPDX-License-Identifier: Apache-2.0

// Package pgtest provides Postgres for integration tests: a Testcontainers
// server per test package (or FOREMAN_TEST_DATABASE_URL) and a freshly
// migrated schema per test.
//
//	var db *pgtest.Database
//	func TestMain(m *testing.M) { os.Exit(pgtest.Main(m, &db)) }
//	pool := db.Pool(t); schema := db.Schema(t, pool)
package pgtest
