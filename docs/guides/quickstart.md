# Quickstart

Run a Foreman job end to end in five commands. You need Git, Go and Docker with Compose v2.

```sh
git clone https://github.com/thnhdatle/foreman && cd foreman
docker compose -f deploy/docker-compose.yml up -d --wait postgres    # Postgres 17 on localhost:55432
export FOREMAN_DATABASE_URL=postgres://foreman:foreman@localhost:55432/foreman?sslmode=disable
go run ./examples/basic                                              # insert and run one job
docker compose -f deploy/docker-compose.yml down -v                  # stop Postgres, delete its volume
```

[`examples/basic`](../../examples/basic/main.go) inserts a user row and an email job in one
transaction, then waits for the job. The last line starts with `job completed`:

```text
enqueued job 1
sending "welcome" email to user 1 (attempt 1)
job completed: id=1 state=completed
```

You can run the example again: it recreates its schema each time.

## Next steps

- [Getting started](getting-started.md): add Foreman to your application.
- [Workers](workers.md): results, retries, timeouts, shutdown.
- [Transactional enqueue](transactional-enqueue.md): commit a job with your data.
- [Configuration](configuration.md): every option and default.
