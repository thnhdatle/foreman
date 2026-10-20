# Concurrency and fairness

Limit how many jobs run at once, per queue and per key, and share capacity fairly between keys.

## Queues and MaxWorkers

```go
Queues: map[string]foreman.QueueConfig{
	"default": {MaxWorkers: 50},                               // up to 50 at once in this client
	"reports": {MaxWorkers: 4, FetchInterval: 5 * time.Second},
},
```

Each queue runs independently. `MaxWorkers` defaults to 10. A queue's total concurrency is the
sum over all clients serving it. Jobs run by priority (1 highest to 4 lowest, default 2), then
schedule time.

## Concurrency keys

```go
client.Insert(ctx, SyncArgs{AccountID: acct}, &foreman.InsertOpts{
	ConcurrencyKey: "account-" + strconv.FormatInt(acct, 10), // at most 255 bytes
})
```

Group jobs by tenant, account or external API. Jobs without a key are not limited by
`PerKeyLimit`.

## PerKeyLimit

```go
"sync": {MaxWorkers: 100, PerKeyLimit: 2}, // at most 2 jobs per key at once, cluster-wide
```

Use the same `PerKeyLimit` on every client serving the queue.

## Fairness

```go
"shared": {MaxWorkers: 10, Fairness: true}, // split free capacity between keys
```

Without fairness, one tenant with 100,000 queued jobs delays everyone. With it, 10 free slots
and keys A, B and C get 3 or 4 jobs each instead of 10 for A. Higher priorities are still served
first.

Fairness only matters when jobs are backlogged. It costs an extra index scan per fetch, so enable
it only on queues shared by many tenants.

## Combining the settings

| Goal | Settings |
| --- | --- |
| Protect a rate-limited API per account | `ConcurrencyKey` per account, `PerKeyLimit` |
| Stop one tenant from starving others | `ConcurrencyKey` per tenant, `Fairness: true` |
| Both | `PerKeyLimit` and `Fairness: true` |
| Isolate slow job types | a separate queue with its own `MaxWorkers` |

With `PerKeyLimit` and many keys, raise Postgres `max_locks_per_transaction` (for example to 256)
if you see "out of shared memory" errors. See [operations](operations.md).
