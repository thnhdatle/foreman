# Unique jobs

Skip inserting a job while another job with the same identity exists.

## Usage

```go
res, err := client.Insert(ctx, ReportArgs{AccountID: 7}, &foreman.InsertOpts{
	UniqueOpts: &foreman.UniqueOpts{ByArgs: true, ByPeriod: time.Hour},
})
if err != nil {
	return err
}
if res.UniqueSkippedAsDuplicate { // the existing job is returned, not an error
	log.Printf("report already queued as job %d", res.Job.ID)
}
```

Uniqueness holds under concurrent inserts, in every insert path including `foreman_insert_job`.

## UniqueOpts

| Field | Adds to the identity |
| --- | --- |
| (always) | the job kind |
| `ByArgs` | all args |
| `ByArgFields` | only these top-level args fields |
| `ByPeriod` | the current period start (at least 1s) |
| `ByQueue` | the queue name |
| `ByState` | not identity: states where enforced |

`ByArgs` and `ByArgFields` are mutually exclusive. With neither, all jobs of the kind share one
identity.

`ByState` defaults to `available`, `scheduled`, `running`, `retryable` and `completed`. A custom
list must include `available`, `scheduled`, `running` and `retryable`.

- Default: no duplicate while one is pending or completed jobs are kept (24h). Add `ByPeriod`
  for once per period.
- The four pending states only: a new job is allowed once the previous one completed.
- Add `cancelled` and `discarded` to block re-insertion after a failure.

`ByPeriod: 24 * time.Hour` buckets by UTC day.

## Identity and canonical form

Numbers in the identity must be integers. A fraction (for example `"amount": 9.99` with `ByArgs`)
fails with `ErrUniqueArgsNotCanonical`. Store decimals as strings (`"9.99"`), or leave the field
out with `ByArgFields`.

The key is `JobRow.UniqueKey` (hex `unique_key` in the admin API). Go and SQL inserts compute it
the same way. Test vectors are in `testdata/contracts/unique/*.json`.

## Interactions

- `JobRetry` fails with `ErrUniqueConflict` if another job holds the key.
- Periodic jobs can use `UniqueOpts` to avoid overlap; see
  [periodic jobs](periodic-jobs.md#exactly-once-per-tick).
- Uniqueness does not make execution exactly once; see
  [workers](workers.md#at-least-once-execution).
