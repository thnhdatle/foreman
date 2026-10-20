# Transactional enqueue

Insert a job in the same transaction as your data, so both commit or neither does.

## InsertTx and InsertManyTx

```go
tx, err := pool.Begin(ctx)
if err != nil {
	return err
}
defer tx.Rollback(ctx) // no-op after Commit

var userID int64
if err := tx.QueryRow(ctx,
	`INSERT INTO users (email) VALUES ($1) RETURNING id`, email).Scan(&userID); err != nil {
	return err
}
if _, err := client.InsertTx(ctx, tx, EmailArgs{UserID: userID, Tmpl: "welcome"}, nil); err != nil {
	return err
}
return tx.Commit(ctx)
```

`InsertManyTx(ctx, tx, []foreman.InsertParams{...})` inserts a batch. Results follow the order of
the params.

- `tx` is any `pgx.Tx` on the Foreman database. A nil `tx` returns `ErrInvalidArgument`.
- An insert error aborts the transaction, like any SQL error.
- [Unique jobs](unique-jobs.md) work the same.

## Wake-ups happen on commit

Workers are woken when the transaction commits. A rollback wakes nobody. `PollOnly` clients find
the job at their next poll.

## Completing a job in your transaction

`CompleteTx` marks the job completed in the transaction that holds its effects:

```go
func (w *ChargeWorker) Work(ctx context.Context, job *foreman.Job[ChargeArgs]) error {
	tx, err := w.pool.Begin(ctx)
	if err != nil {
		return err
	}
	defer tx.Rollback(ctx)
	_, err = tx.Exec(ctx, `UPDATE invoices SET paid = true WHERE id = $1`, job.Args.InvoiceID)
	if err != nil {
		return err
	}
	if err := foreman.CompleteTx(ctx, tx, job); err != nil {
		return err // ErrStaleJob: this attempt no longer owns the job
	}
	return tx.Commit(ctx) // a commit error is returned, so the attempt is retried
}
```

- Call it from `Work`, with the job context and the job being worked. Otherwise it returns
  `ErrNotInWorker`.
- On `ErrStaleJob` it wrote nothing. Roll back.
- It locks the job row until `tx` ends. Keep the transaction short.
- Return nil only after a successful commit. Otherwise the job stays `running` until it is
  rescued.

## Comparison with the outbox pattern

| | Outbox and broker | Foreman |
| --- | --- | --- |
| Atomic with business writes | yes (outbox row) | yes (job row) |
| Extra moving parts | relay, broker | none |
| Delivery latency | relay poll interval + broker | NOTIFY on commit (poll in `PollOnly`) |
| Consumer side atomicity | inbox or dedupe table | `CompleteTx` for same-database effects |
| Retries, scheduling, dead letters | broker or consumer specific | built in |

Effects outside the database (emails, HTTP calls) still run at least once. Make them idempotent;
see [workers](workers.md#at-least-once-execution).

## Inserting from other languages

Use the SQL function `foreman_insert_job`; see
[insert from other languages](insert-from-other-languages.md).
