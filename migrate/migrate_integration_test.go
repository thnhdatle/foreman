// SPDX-License-Identifier: Apache-2.0

//go:build integration

package migrate_test

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"os"
	"path/filepath"
	"sync"
	"testing"

	"github.com/jackc/pgx/v5/pgconn"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/thnhdatle/foreman/migrate"
)

func TestMigrate_UpDownUpAgain(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	pool := db.Pool(t)
	schema := "mig_updown"
	t.Cleanup(func() { _, _ = pool.Exec(ctx, `DROP SCHEMA IF EXISTS mig_updown CASCADE`) })
	opt := migrate.WithSchema(schema)

	v, err := migrate.Version(ctx, pool, opt)
	require.NoError(t, err)
	assert.Equal(t, 0, v)

	var wg sync.WaitGroup
	errs := make([]error, 4)
	for i := range errs {
		wg.Add(1)
		go func() {
			defer wg.Done()
			errs[i] = migrate.Up(ctx, pool, opt)
		}()
	}
	wg.Wait()
	for _, err := range errs {
		require.NoError(t, err)
	}
	v, err = migrate.Version(ctx, pool, opt)
	require.NoError(t, err)
	assert.Equal(t, migrate.Latest(), v)
	require.NoError(t, migrate.Up(ctx, pool, opt), "idempotent")

	require.NoError(t, migrate.Down(ctx, pool, opt))
	v, err = migrate.Version(ctx, pool, opt)
	require.NoError(t, err)
	assert.Equal(t, 0, v)
	var tables int
	require.NoError(t, pool.QueryRow(ctx, `SELECT count(*) FROM pg_tables WHERE schemaname = $1 AND tablename <> 'foreman_migrations'`,
		schema).Scan(&tables))
	assert.Zero(t, tables, "down removes every Foreman table")

	require.NoError(t, migrate.Up(ctx, pool, opt))
	v, err = migrate.Version(ctx, pool, opt)
	require.NoError(t, err)
	assert.Equal(t, migrate.Latest(), v)
}

func TestMigrate_SQLMatchesUp(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	pool := db.Pool(t)
	t.Cleanup(func() { _, _ = pool.Exec(ctx, `DROP SCHEMA IF EXISTS mig_sql CASCADE`) })
	up, err := migrate.SQL(true, migrate.WithSchema("mig_sql"))
	require.NoError(t, err)
	_, err = pool.Exec(ctx, up)
	require.NoError(t, err, "printed SQL applies cleanly")
	v, err := migrate.Version(ctx, pool, migrate.WithSchema("mig_sql"))
	require.NoError(t, err)
	assert.Equal(t, migrate.Latest(), v)
	down, err := migrate.SQL(false, migrate.WithSchema("mig_sql"))
	require.NoError(t, err)
	_, err = pool.Exec(ctx, down)
	require.NoError(t, err)
}

func TestMigrate_UnknownVersion(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	pool := db.Pool(t)
	schema := db.Schema(t, pool)
	_, err := pool.Exec(ctx, `INSERT INTO "`+schema+`".foreman_migrations (version, name) VALUES (999, 'future')`)
	require.NoError(t, err)
	_, err = migrate.Version(ctx, pool, migrate.WithSchema(schema))
	require.ErrorIs(t, err, migrate.ErrUnknownVersion)
}

type vector struct {
	Identity  json.RawMessage `json:"identity"`
	Canonical string          `json:"canonical"`
	SHA256    string          `json:"sha256"`
	Error     string          `json:"error"`
}

func loadVectors(t *testing.T) map[string]vector {
	t.Helper()
	paths, err := filepath.Glob("../testdata/contracts/unique/*.json")
	require.NoError(t, err)
	require.NotEmpty(t, paths)
	out := map[string]vector{}
	for _, p := range paths {
		data, err := os.ReadFile(p)
		require.NoError(t, err)
		var v vector
		require.NoError(t, json.Unmarshal(data, &v))
		out[filepath.Base(p)] = v
	}
	return out
}

func TestCanonicalJSON_GoldenVectors(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	pool := db.Pool(t)
	schema := db.Schema(t, pool)
	for name, v := range loadVectors(t) {
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			var canonical string
			var sum []byte
			err := pool.QueryRow(ctx, `SELECT "`+schema+`".foreman_canonical_json($1::text::jsonb),
				sha256(convert_to("`+schema+`".foreman_canonical_json($1::text::jsonb), 'UTF8'))`,
				string(v.Identity)).Scan(&canonical, &sum)
			if v.Error != "" {
				var pgErr *pgconn.PgError
				require.ErrorAs(t, err, &pgErr)
				assert.Equal(t, "22023", pgErr.Code)
				assert.Contains(t, pgErr.Message, v.Error)
				return
			}
			require.NoError(t, err)
			assert.Equal(t, v.Canonical, canonical)
			assert.Equal(t, v.SHA256, hex.EncodeToString(sum))
			goSum := sha256.Sum256([]byte(v.Canonical))
			assert.Equal(t, v.SHA256, hex.EncodeToString(goSum[:]), "vector file is self-consistent")
		})
	}
}
