// SPDX-License-Identifier: Apache-2.0

// Package exampledb connects the examples to Postgres with a freshly migrated schema.
package exampledb

import (
	"context"
	"fmt"
	"os"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/thnhdatle/foreman/migrate"
)

//nolint:gosec // G101: credentials of the local compose database, not a secret
const DefaultURL = "postgres://foreman:foreman@localhost:55432/foreman?sslmode=disable"

// Open connects to the database and recreates schema with the migrations applied.
func Open(ctx context.Context, schema string) (*pgxpool.Pool, error) {
	url := os.Getenv("FOREMAN_DATABASE_URL")
	if url == "" {
		url = DefaultURL
	}
	pool, err := pgxpool.New(ctx, url)
	if err != nil {
		return nil, fmt.Errorf("connect: %w", err)
	}
	if _, err := pool.Exec(ctx, "DROP SCHEMA IF EXISTS "+pgx.Identifier{schema}.Sanitize()+" CASCADE"); err != nil {
		pool.Close()
		return nil, fmt.Errorf("reset schema: %w", err)
	}
	if err := migrate.Up(ctx, pool, migrate.WithSchema(schema)); err != nil {
		pool.Close()
		return nil, fmt.Errorf("migrate: %w", err)
	}
	return pool, nil
}
