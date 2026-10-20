# Insert from other languages

Enqueue Foreman jobs from any language with plain SQL, in your own transactions.

```sql
foreman_insert_job(kind text, args jsonb, opts jsonb DEFAULT '{}') RETURNS bigint
```

It returns the new job id, or the existing one for a duplicate [unique job](unique-jobs.md).
Outside `public`, add the schema prefix: `jobs.foreman_insert_job(...)`.

## Options

Every key of `opts` is optional:

| Key | Type | Default |
| --- | --- | --- |
| `queue` | string | `"default"` |
| `priority` | integer 1 (highest) to 4 | `2` |
| `scheduled_at` | timestamp string, for example `"2026-10-18T12:00:00Z"` | now |
| `max_attempts` | integer >= 1 | `25` |
| `tags` | array of strings | `[]` |
| `metadata` | object | `{}` |
| `concurrency_key` | string, empty means none | none |
| `timeout_ms` | integer milliseconds | none (kind or client default) |
| `unique` | object, see below | not unique |

`unique` mirrors `UniqueOpts`:

| Key | Type | Meaning |
| --- | --- | --- |
| `by_args` | boolean | all args |
| `by_arg_fields` | array of strings | these top-level fields |
| `by_period_seconds` | integer | current period start |
| `by_queue` | boolean | the queue |
| `by_state` | array of state names | states where enforced |

Times use the database clock, `now()`: the start of the current transaction.

## psql

```sql
SELECT foreman_insert_job('email', '{"user_id": 42, "tmpl": "welcome"}');

SELECT foreman_insert_job(
  'report',
  '{"account_id": 7}',
  '{"queue": "reports", "priority": 1, "concurrency_key": "account-7",
    "unique": {"by_args": true, "by_period_seconds": 3600}}'
);
```

## Python (psycopg 3)

```python
import psycopg
from psycopg.types.json import Jsonb

with psycopg.connect("postgresql://foreman:foreman@localhost:55432/foreman") as conn:
    with conn.transaction():  # the user row and the job commit together
        user_id = conn.execute(
            "INSERT INTO users (email) VALUES (%s) RETURNING id", ("ada@example.com",)
        ).fetchone()[0]
        job_id = conn.execute(
            "SELECT foreman_insert_job(%s, %s, %s)",
            (
                "email",
                Jsonb({"user_id": user_id, "tmpl": "welcome"}),
                Jsonb({"queue": "default", "unique": {"by_args": True}}),
            ),
        ).fetchone()[0]
    print("enqueued job", job_id)
```

The args must match the JSON encoding of the Go args type.

## Errors

SQLSTATE `22023` (`invalid_parameter_value`):

| Message | Cause |
| --- | --- |
| `foreman: kind and args are required` | `kind` or `args` is NULL |
| `foreman: invalid unique options: by_args and by_arg_fields are mutually exclusive` | both set |
| `foreman: invalid unique options: by_state must include available, scheduled, running and retryable` | incomplete `by_state` |
| `foreman: invalid unique options: by_state contains an unknown state` | unknown state name |
| `foreman: unique args not canonical: non-integral number <n>` | fraction in the identity |

A wrong type in `opts` raises `22P02`. Values out of range (priority outside 1..4,
`max_attempts` below 1, empty kind) raise `23514`.

## Differences from Go inserts

The function does not:

- check kind and queue names, or `Config.MaxArgsBytes`;
- apply per-kind defaults such as `WithMaxAttempts`, or `JobArgsWithInsertOpts`;
- replace a past `scheduled_at` with the insert time;
- run Go middleware or update metrics.

Validate input in the calling service. Insert only kinds a Go worker is registered for.
