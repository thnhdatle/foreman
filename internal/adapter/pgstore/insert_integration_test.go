// SPDX-License-Identifier: Apache-2.0

//go:build integration

package pgstore_test

import (
	"context"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"pgregory.net/rapid"

	"github.com/thnhdatle/foreman/internal/adapter/pgstore"
	"github.com/thnhdatle/foreman/internal/domain/job"
	"github.com/thnhdatle/foreman/internal/domain/unique"
)

func TestInsert_SingleAndBatchPathsKeepOrder(t *testing.T) {
	t.Parallel()
	for _, n := range []int{1, pgstore.CopyThreshold - 1, pgstore.CopyThreshold, 250} {
		t.Run(fmt.Sprint(n), func(t *testing.T) {
			t.Parallel()
			e := newEnv(t)
			ps := make([]job.InsertParams, n)
			for i := range ps {
				ps[i] = params("k", fmt.Sprintf("q%d", i%3))
				ps[i].Args = []byte(fmt.Sprintf(`{"i":%d}`, i))
				ps[i].Priority = 1 + i%4
				ps[i].Tags = []string{"t", fmt.Sprint(i)}
				ps[i].Metadata = []byte(`{"m":true}`)
				if i%2 == 0 {
					ps[i].ConcurrencyKey = "tenant"
					ps[i].Timeout = 1500 * time.Millisecond
				}
				if i%5 == 0 {
					ps[i].State = job.StateScheduled
					ps[i].ScheduledAt = t0.Add(time.Hour)
				}
			}
			res := e.insert(t, ps...)
			require.Len(t, res, n)
			for i, r := range res {
				assert.False(t, r.Skipped)
				var args map[string]int
				require.NoError(t, json.Unmarshal(r.Row.Args, &args))
				assert.Equal(t, i, args["i"], "results follow input order")
				assert.Equal(t, ps[i].Queue, r.Row.Queue)
				assert.Equal(t, ps[i].Priority, r.Row.Priority)
				assert.Equal(t, ps[i].State, r.Row.State)
				assert.Equal(t, ps[i].Tags, r.Row.Tags)
				assert.JSONEq(t, `{"m":true}`, string(r.Row.Metadata))
				assert.Equal(t, ps[i].ConcurrencyKey, r.Row.ConcurrencyKey)
				assert.Equal(t, ps[i].Timeout, r.Row.Timeout)
				assert.True(t, ps[i].ScheduledAt.Equal(r.Row.ScheduledAt))
				assert.Equal(t, 0, r.Row.Attempt)
				assert.Equal(t, 3, r.Row.MaxAttempts)
				assert.Empty(t, r.Row.Errors)
				assert.Nil(t, r.Row.UniqueKey)
			}
			assert.Equal(t, n, e.count(t, "true"))
		})
	}
}

func TestInsert_InCallerTransactionOnlyOnCommit(t *testing.T) {
	t.Parallel()
	e := newEnv(t)
	ctx := context.Background()
	for _, n := range []int{1, 20} {
		tx, err := e.pool.Begin(ctx)
		require.NoError(t, err)
		ps := make([]job.InsertParams, n)
		for i := range ps {
			ps[i] = params("k", "q")
		}
		_, err = e.store.Insert(ctx, tx, ps)
		require.NoError(t, err)
		_, err = e.store.Insert(ctx, tx, ps) // a second batch in the same transaction
		require.NoError(t, err)
		require.NoError(t, tx.Rollback(ctx))
	}
	assert.Zero(t, e.count(t, "true"), "rolled back inserts leave nothing")
	tx, err := e.pool.Begin(ctx)
	require.NoError(t, err)
	_, err = e.store.Insert(ctx, tx, []job.InsertParams{params("k", "q")})
	require.NoError(t, err)
	require.NoError(t, tx.Commit(ctx))
	assert.Equal(t, 1, e.count(t, "true"))
}

func TestInsert_UniqueSkipsDuplicates(t *testing.T) {
	t.Parallel()
	e := newEnv(t)
	byArgs := unique.Options{ByArgs: true}
	first := e.insert(t, uniqueParams("u", `{"a":1}`, byArgs))[0]
	assert.False(t, first.Skipped)
	require.NotNil(t, first.Row.UniqueKey)
	assert.Equal(t, unique.DefaultStates, first.Row.UniqueStates)

	again := e.insert(t, uniqueParams("u", `{"a":1.0}`, byArgs))[0]
	assert.True(t, again.Skipped, "1.0 and 1 are the same identity")
	assert.Equal(t, first.Row.ID, again.Row.ID)

	other := e.insert(t, uniqueParams("u", `{"a":2}`, byArgs))[0]
	assert.False(t, other.Skipped)

	// A batch through COPY with duplicates inside the batch and of existing jobs.
	var batch []job.InsertParams
	for i := 0; i < 12; i++ {
		batch = append(batch, uniqueParams("u", fmt.Sprintf(`{"a":%d}`, i%4), byArgs))
	}
	res := e.insert(t, batch...)
	ids := map[int64]bool{}
	inserted := 0
	for i, r := range res {
		ids[r.Row.ID] = true
		if !r.Skipped {
			inserted++
		}
		var a map[string]int
		require.NoError(t, json.Unmarshal(r.Row.Args, &a))
		assert.Equal(t, i%4, a["a"])
	}
	assert.Equal(t, 2, inserted, "only a=0 and a=3 are new")
	assert.Len(t, ids, 4)
	assert.Equal(t, 4, e.count(t, "true"))
}

func TestInsert_UniqueStatesAndPeriod(t *testing.T) {
	t.Parallel()
	e := newEnv(t)
	pendingOnly := unique.Options{ByArgs: true, ByState: job.PendingStates}
	first := e.insert(t, uniqueParams("u", `{}`, pendingOnly))[0]
	e.exec(t, `UPDATE "`+e.schema+`".foreman_jobs SET state = 'completed', finalized_at = $1 WHERE id = $2`, t0, first.Row.ID)
	second := e.insert(t, uniqueParams("u", `{}`, pendingOnly))[0]
	assert.False(t, second.Skipped, "completed is outside ByState, so a new job is allowed")

	withCompleted := unique.Options{ByArgs: true}
	a := e.insert(t, uniqueParams("v", `{}`, withCompleted))[0]
	e.exec(t, `UPDATE "`+e.schema+`".foreman_jobs SET state = 'completed', finalized_at = $1 WHERE id = $2`, t0, a.Row.ID)
	b := e.insert(t, uniqueParams("v", `{}`, withCompleted))[0]
	assert.True(t, b.Skipped, "completed is covered by the default states")
	assert.Equal(t, job.StateCompleted, b.Row.State)
}

func TestInsert_NonCanonicalArgsRejected(t *testing.T) {
	t.Parallel()
	e := newEnv(t)
	_, err := e.store.Insert(context.Background(), e.pool, []job.InsertParams{uniqueParams("u", `{"x":1.5}`, unique.Options{ByArgs: true})})
	require.ErrorIs(t, err, unique.ErrArgsNotCanonical)
	assert.Zero(t, e.count(t, "true"))
	// Non-unique jobs may carry any number.
	e.insert(t, func() job.InsertParams { p := params("k", "q"); p.Args = []byte(`{"x":1.5}`); return p }())
}

type vector struct {
	Identity json.RawMessage `json:"identity"`
	SHA256   string          `json:"sha256"`
	Error    string          `json:"error"`
}

// Golden vectors through both insert paths: the Go client (identity built by
// internal/domain/unique) and foreman_insert_job (identity built in SQL).
func TestInsert_GoldenVectorsBothPaths(t *testing.T) {
	t.Parallel()
	e := newEnv(t)
	ctx := context.Background()
	paths, err := filepath.Glob("../../../testdata/contracts/unique/*.json")
	require.NoError(t, err)
	require.NotEmpty(t, paths)
	for _, p := range paths {
		data, err := os.ReadFile(p)
		require.NoError(t, err)
		var v vector
		require.NoError(t, json.Unmarshal(data, &v))
		var ident struct {
			Kind   string          `json:"kind"`
			Args   json.RawMessage `json:"args"`
			Queue  *string         `json:"queue"`
			Period *int64          `json:"period"`
		}
		require.NoError(t, json.Unmarshal(v.Identity, &ident))
		if ident.Queue != nil || ident.Period != nil {
			continue // identities with queue/period depend on insert time; covered in the SQL vector test
		}
		name := filepath.Base(p)
		gp := params(ident.Kind, "default")
		gp.Args = ident.Args
		gp.UniqueIdentity, err = unique.Identity(ident.Kind, "default", ident.Args, unique.Options{ByArgs: true}, t0)
		require.NoError(t, err)
		gp.UniqueStates = unique.Mask(unique.DefaultStates)
		res, goErr := e.store.Insert(ctx, e.pool, []job.InsertParams{gp})

		var sqlID int64
		sqlErr := e.pool.QueryRow(ctx, `SELECT "`+e.schema+`".foreman_insert_job($1, $2::text::jsonb, '{"unique":{"by_args":true}}')`,
			ident.Kind, string(ident.Args)).Scan(&sqlID)
		if v.Error != "" {
			require.ErrorIs(t, goErr, unique.ErrArgsNotCanonical, name)
			require.ErrorContains(t, sqlErr, v.Error, name)
			continue
		}
		require.NoError(t, goErr, name)
		require.NoError(t, sqlErr, name)
		assert.Equal(t, v.SHA256, hex.EncodeToString(res[0].Row.UniqueKey), name)
		assert.Equal(t, res[0].Row.ID, sqlID, "%s: foreman_insert_job found the Go-inserted job", name)
	}
}

func TestInsertJobFunction_Options(t *testing.T) {
	t.Parallel()
	e := newEnv(t)
	ctx := context.Background()
	fn := `"` + e.schema + `".foreman_insert_job`
	var id int64
	require.NoError(t, e.pool.QueryRow(ctx, `SELECT `+fn+`('email', '{"to":"a"}', $1)`, `{
		"queue":"mail","priority":1,"max_attempts":5,"tags":["x","y"],"metadata":{"src":"py"},
		"concurrency_key":"t1","timeout_ms":2500,"scheduled_at":"2999-01-01T00:00:00Z"}`).Scan(&id))
	r, err := e.store.JobGet(ctx, id)
	require.NoError(t, err)
	assert.Equal(t, "mail", r.Queue)
	assert.Equal(t, 1, r.Priority)
	assert.Equal(t, 5, r.MaxAttempts)
	assert.Equal(t, []string{"x", "y"}, r.Tags)
	assert.JSONEq(t, `{"src":"py"}`, string(r.Metadata))
	assert.Equal(t, "t1", r.ConcurrencyKey)
	assert.Equal(t, 2500*time.Millisecond, r.Timeout)
	assert.Equal(t, job.StateScheduled, r.State)

	require.NoError(t, e.pool.QueryRow(ctx, `SELECT `+fn+`('email', '{}')`).Scan(&id))
	r, err = e.store.JobGet(ctx, id)
	require.NoError(t, err)
	assert.Equal(t, "default", r.Queue)
	assert.Equal(t, 2, r.Priority)
	assert.Equal(t, 25, r.MaxAttempts)
	assert.Equal(t, job.StateAvailable, r.State)

	// Unique by selected fields and queue: the Go and SQL identities agree.
	opts := unique.Options{ByArgFields: []string{"user"}, ByQueue: true}
	goRes := e.insert(t, uniqueParams("welcome", `{"user":9007199254740993,"noise":1}`, opts))[0]
	require.NoError(t, e.pool.QueryRow(ctx, `SELECT `+fn+`('welcome', '{"noise":2,"user":9007199254740993}',
		'{"unique":{"by_arg_fields":["user"],"by_queue":true}}')`).Scan(&id))
	assert.Equal(t, goRes.Row.ID, id)
	require.NoError(t, e.pool.QueryRow(ctx, `SELECT `+fn+`('welcome', '{"user":9007199254740992}',
		'{"unique":{"by_arg_fields":["user"],"by_queue":true}}')`).Scan(&id))
	assert.NotEqual(t, goRes.Row.ID, id, "ids above 2^53 that differ in the last digit are distinct")

	_, err = e.pool.Exec(ctx, `SELECT `+fn+`('k', '{}', '{"unique":{"by_args":true,"by_arg_fields":["a"]}}')`)
	require.ErrorContains(t, err, "mutually exclusive")
	_, err = e.pool.Exec(ctx, `SELECT `+fn+`('k', '{}', '{"unique":{"by_state":["available"]}}')`)
	require.ErrorContains(t, err, "by_state must include")
	_, err = e.pool.Exec(ctx, `SELECT `+fn+`('k', '{}',
		'{"unique":{"by_state":["available","scheduled","running","retryable","zombie"]}}')`)
	require.ErrorContains(t, err, "unknown state")
	_, err = e.pool.Exec(ctx, `SELECT `+fn+`(NULL, '{}')`)
	require.ErrorContains(t, err, "kind and args are required")
}

func TestInsertJobFunction_PeriodBucketMatchesGo(t *testing.T) {
	t.Parallel()
	e := newEnv(t)
	ctx := context.Background()
	var now time.Time
	require.NoError(t, e.pool.QueryRow(ctx, `SELECT now()`).Scan(&now))
	opts := unique.Options{ByArgs: true, ByPeriod: 24 * time.Hour}
	p := uniqueParams("daily", `{"r":1}`, unique.Options{})
	var err error
	p.UniqueIdentity, err = unique.Identity("daily", "default", p.Args, opts, now)
	require.NoError(t, err)
	res := e.insert(t, p)[0]
	var id int64
	require.NoError(t, e.pool.QueryRow(ctx, `SELECT "`+e.schema+`".foreman_insert_job('daily', '{"r":1}',
		'{"unique":{"by_args":true,"by_period_seconds":86400}}')`).Scan(&id))
	if unique.PeriodBucket(now, 24*time.Hour) == unique.PeriodBucket(time.Now(), 24*time.Hour) {
		assert.Equal(t, res.Row.ID, id)
	}
}

var iteration atomic.Int64

// Property: concurrent inserts with the same unique key leave exactly one row.
func TestInsert_Property_ConcurrentUniqueInsertsLeaveOneRow(t *testing.T) {
	t.Parallel()
	e := newEnv(t)
	ctx := context.Background()
	fn := `"` + e.schema + `".foreman_insert_job`
	rapid.Check(t, func(rt *rapid.T) {
		keys := rapid.IntRange(1, 5).Draw(rt, "keys")
		workers := rapid.IntRange(2, 12).Draw(rt, "workers")
		perWorker := rapid.IntRange(1, 6).Draw(rt, "perWorker")
		run := iteration.Add(1) // rapid replays drawn values while shrinking; keep runs apart
		var wg sync.WaitGroup
		errs := make(chan error, workers*perWorker)
		for w := 0; w < workers; w++ {
			wg.Add(1)
			go func() {
				defer wg.Done()
				for i := 0; i < perWorker; i++ {
					args := fmt.Sprintf(`{"run":%d,"k":%d}`, run, (w+i)%keys)
					if (w+i)%2 == 0 {
						_, err := e.store.Insert(ctx, e.pool, []job.InsertParams{uniqueParams("race", args, unique.Options{ByArgs: true})})
						errs <- err
					} else {
						_, err := e.pool.Exec(ctx, `SELECT `+fn+`('race', $1::text::jsonb, '{"unique":{"by_args":true}}')`, args)
						errs <- err
					}
				}
			}()
		}
		wg.Wait()
		close(errs)
		for err := range errs {
			if err != nil {
				rt.Fatal(err)
			}
		}
		var n int
		if err := e.pool.QueryRow(ctx, `SELECT count(*) FROM "`+e.schema+`".foreman_jobs WHERE (args->>'run')::bigint = $1`, run).Scan(&n); err != nil {
			rt.Fatal(err)
		}
		want := distinctKeys(workers, perWorker, keys)
		if n != want {
			rt.Fatalf("%d rows for %d keys (want %d)", n, keys, want)
		}
	})
}

func distinctKeys(workers, perWorker, keys int) int {
	seen := map[int]bool{}
	for w := 0; w < workers; w++ {
		for i := 0; i < perWorker; i++ {
			seen[(w+i)%keys] = true
		}
	}
	return len(seen)
}

// jsonValue generates random JSON with integral numbers in several spellings.
func jsonValue(depth int) *rapid.Generator[string] {
	return rapid.Custom(func(t *rapid.T) string {
		kind := rapid.IntRange(0, 5).Draw(t, "kind")
		if depth <= 0 && kind >= 4 {
			kind = 2
		}
		switch kind {
		case 0:
			return rapid.SampledFrom([]string{"true", "false", "null"}).Draw(t, "lit")
		case 1:
			// jsonb cannot store U+0000.
			s := strings.ReplaceAll(rapid.String().Draw(t, "s"), "\x00", "")
			b, _ := json.Marshal(s)
			return string(b)
		case 2, 3:
			return fmt.Sprint(rapid.Int64().Draw(t, "n"))
		case 4:
			n := rapid.IntRange(0, 4).Draw(t, "len")
			parts := make([]string, n)
			for i := range parts {
				parts[i] = jsonValue(depth-1).Draw(t, "el")
			}
			return "[" + strings.Join(parts, ",") + "]"
		default:
			n := rapid.IntRange(0, 4).Draw(t, "len")
			seen := map[string]bool{}
			var parts []string
			for i := 0; i < n; i++ {
				k := rapid.StringMatching(`[a-zé日"\\]{0,4}`).Draw(t, "key")
				if seen[k] {
					continue
				}
				seen[k] = true
				kb, _ := json.Marshal(k)
				parts = append(parts, string(kb)+":"+jsonValue(depth-1).Draw(t, "v"))
			}
			return "{" + strings.Join(parts, ",") + "}"
		}
	})
}

// rewrite permutes object keys and respells integers as n, n.0 or ne0.
func rewrite(t *rapid.T, v any) string {
	switch x := v.(type) {
	case map[string]any:
		keys := make([]string, 0, len(x))
		for k := range x {
			keys = append(keys, k)
		}
		perm := rapid.Permutation(keys).Draw(t, "perm")
		parts := make([]string, len(perm))
		for i, k := range perm {
			kb, _ := json.Marshal(k)
			parts[i] = string(kb) + ":" + rewrite(t, x[k])
		}
		return "{" + strings.Join(parts, ",") + "}"
	case []any:
		parts := make([]string, len(x))
		for i, el := range x {
			parts[i] = rewrite(t, el)
		}
		return "[" + strings.Join(parts, ",") + "]"
	case json.Number:
		switch rapid.IntRange(0, 2).Draw(t, "spelling") {
		case 1:
			return x.String() + ".0"
		case 2:
			return x.String() + "e0"
		default:
			return x.String()
		}
	default:
		b, _ := json.Marshal(x)
		return string(b)
	}
}

// Property: permuting keys and respelling integers never changes
// foreman_canonical_json; distinct integers never collide.
func TestCanonicalJSON_Property_PermutationAndNumberSpelling(t *testing.T) {
	t.Parallel()
	e := newEnv(t)
	ctx := context.Background()
	q := `SELECT "` + e.schema + `".foreman_canonical_json($1::text::jsonb)`
	rapid.Check(t, func(rt *rapid.T) {
		original := jsonValue(3).Draw(rt, "json")
		dec := json.NewDecoder(strings.NewReader(original))
		dec.UseNumber()
		var parsed any
		if err := dec.Decode(&parsed); err != nil {
			rt.Fatal(err)
		}
		variant := rewrite(rt, parsed)
		var a, b string
		if err := e.pool.QueryRow(ctx, q, original).Scan(&a); err != nil {
			rt.Fatal(err)
		}
		if err := e.pool.QueryRow(ctx, q, variant).Scan(&b); err != nil {
			rt.Fatal(err)
		}
		if a != b {
			rt.Fatalf("canonical forms differ:\n%s -> %s\n%s -> %s", original, a, variant, b)
		}
		x, y := rapid.Int64().Draw(rt, "x"), rapid.Int64().Draw(rt, "y")
		if x == y {
			return
		}
		var cx, cy string
		if err := e.pool.QueryRow(ctx, q, fmt.Sprint(x)).Scan(&cx); err != nil {
			rt.Fatal(err)
		}
		if err := e.pool.QueryRow(ctx, q, fmt.Sprint(y)).Scan(&cy); err != nil {
			rt.Fatal(err)
		}
		if cx == cy {
			rt.Fatalf("distinct integers %d and %d collide", x, y)
		}
	})
}
