# Errors

Look up any Foreman Go error, admin HTTP error code or CLI exit code. All are public API.

## Go errors

```go
if errors.Is(err, foreman.ErrNotFound) { // errors are wrapped: compare with errors.Is
	// ...
}
```

| Error | Meaning |
| --- | --- |
| `ErrArgsTooLarge` | Args over `Config.MaxArgsBytes` (default 1 MiB); nothing inserted. |
| `ErrUniqueArgsNotCanonical` | Fraction in the unique identity; use strings ([unique jobs](unique-jobs.md)). |
| `ErrInvalidConfig` | Bad `Config`; lists every problem. |
| `ErrInvalidInsertOpts` | Bad `InsertOpts` or `UniqueOpts`. |
| `ErrInvalidArgument` | Nil or malformed argument. |
| `ErrNotFound` | No such job, or no leader (`LeaderGet`). |
| `ErrInvalidTransition` | Not allowed in the job's state. |
| `ErrUniqueConflict` | Retry would duplicate a pending unique job. |
| `ErrStaleJob` | `CompleteTx`: attempt no longer owned. |
| `ErrNotInWorker` | `CompleteTx` outside `Work` or on another job. |
| `ErrAlreadyStarted` | `Start` on a running client. |
| `ErrNotStarted` | `Stop` or `StopAndCancel` on a stopped client. |
| `ErrSchemaNotMigrated` | Schema too old; run the migrations. |
| `ErrNoWorker` | No worker for the kind; the job is retried. |
| `ErrClientStopping` | `context.Cause` of a job cancelled by shutdown; never returned. |

`migrate.ErrUnknownVersion`: the schema is newer than this binary.

`Snooze`, `Discard` and `Cancel` returned from `Work` are results, not errors
([workers](workers.md#results)).

## SQL errors

`foreman_insert_job` raises SQLSTATE `22023` with messages starting `foreman:`. See
[insert from other languages](insert-from-other-languages.md#errors).

## HTTP error codes

`foremanhttp.Handler` returns this envelope:

```json
{"error": {"code": "not_found", "message": "job not found", "request_id": "01JA7Q3V9F0Z8M5K"}}
```

`request_id` echoes a valid `X-Request-Id` header (1 to 128 characters of `[A-Za-z0-9._-]`), or
is a new ULID. It is also returned in the `X-Request-Id` response header.

| Code | HTTP status | Meaning |
| --- | --- | --- |
| `not_found` | 404 | Unknown job or endpoint, or no leader. |
| `invalid_argument` | 400 | Malformed input; `limit` must be 1..500. |
| `invalid_transition` | 409 | Not allowed in the job's state. |
| `unique_conflict` | 409 | Retry would duplicate a unique job. |
| `internal` | 500 | Any other failure; details hidden. |

## CLI exit codes

| Code | Meaning |
| --- | --- |
| 0 | Success. |
| 1 | Command failed, or unknown command or flag. |
| 2 | Invalid config or database URL, malformed job id, bad `migrate sql` direction, `config print` without `--redact`. |

Errors go to stderr as `error: <message>`.
