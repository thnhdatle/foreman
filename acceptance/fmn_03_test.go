// SPDX-License-Identifier: Apache-2.0

//go:build acceptance

package acceptance

import (
	"context"
	"fmt"
	"sync"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/thnhdatle/foreman"
	"github.com/thnhdatle/foreman/acceptance/internal/accjobs"
)

// FMN-03: jobs inserted with InsertTx exist only if the transaction commits.
func TestAcceptance_FMN03_TransactionalInsert(t *testing.T) {
	th := loadThresholds(t, "FMN-03")
	total := th.Int(t, "transactions")
	rollbackEvery := int(1 / th.Float(t, "rolled_back_ratio"))
	ctx := context.Background()
	const schema = "acc_fmn03"
	pool := newPool(t, dbURL(), 40)
	freshSchema(t, pool, schema)
	_, err := pool.Exec(ctx, `CREATE TABLE "acc_fmn03".acc_users (id bigserial PRIMARY KEY, seq int NOT NULL)`)
	require.NoError(t, err)
	client, err := foreman.NewClient(pool, foreman.Config{Schema: schema, Logger: testLogger(t)})
	require.NoError(t, err)

	const goroutines = 20
	var wg sync.WaitGroup
	errs := make(chan error, goroutines)
	for g := 0; g < goroutines; g++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := g; i < total; i += goroutines {
				if err := enqueueInTx(ctx, client, i, i%rollbackEvery == 0); err != nil {
					errs <- err
					return
				}
			}
		}()
	}
	wg.Wait()
	close(errs)
	for err := range errs {
		require.NoError(t, err)
	}

	rolledBack := (total + rollbackEvery - 1) / rollbackEvery
	committed := total - rolledBack
	var fromRolledBack, fromCommitted, users int64
	require.NoError(t, pool.QueryRow(ctx, `SELECT
		count(*) FILTER (WHERE (args->>'seq')::int % $1 = 0),
		count(*) FILTER (WHERE (args->>'seq')::int % $1 <> 0)
		FROM "acc_fmn03".foreman_jobs`, rollbackEvery).Scan(&fromRolledBack, &fromCommitted))
	require.NoError(t, pool.QueryRow(ctx, `SELECT count(*) FROM "acc_fmn03".acc_users`).Scan(&users))
	ratio := float64(fromCommitted) / float64(committed)
	measured := map[string]any{
		"transactions": total, "rolled_back": rolledBack, "committed": committed,
		"jobs_from_rolled_back": fromRolledBack, "jobs_from_committed": fromCommitted, "committed_jobs_ratio": ratio,
		"business_rows": users,
	}
	pass := fromRolledBack <= int64(th.Int(t, "jobs_from_rolled_back_max")) &&
		ratio >= th.Float(t, "committed_jobs_ratio_min") && users == int64(committed)
	writeResult(t, "FMN-03", measured, pass, nil)
	assert.LessOrEqual(t, fromRolledBack, int64(th.Int(t, "jobs_from_rolled_back_max")))
	assert.GreaterOrEqual(t, ratio, th.Float(t, "committed_jobs_ratio_min"))
	assert.Equal(t, int64(committed), users, "business rows and jobs commit together")
}

// enqueueInTx writes a business row and its job in one transaction and
// commits or rolls back both.
func enqueueInTx(ctx context.Context, c *foreman.Client, seq int, rollback bool) error {
	tx, err := c.Pool().Begin(ctx)
	if err != nil {
		return fmt.Errorf("begin: %w", err)
	}
	defer func() { _ = tx.Rollback(ctx) }()
	if _, err := tx.Exec(ctx, `INSERT INTO "acc_fmn03".acc_users (seq) VALUES ($1)`, seq); err != nil {
		return fmt.Errorf("business row: %w", err)
	}
	if _, err := c.InsertTx(ctx, tx, accjobs.NoopArgs{Seq: int64(seq)}, nil); err != nil {
		return fmt.Errorf("insert job: %w", err)
	}
	if rollback {
		return tx.Rollback(ctx) //nolint:wrapcheck // test helper
	}
	return tx.Commit(ctx) //nolint:wrapcheck // test helper
}
