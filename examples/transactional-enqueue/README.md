# Transactional enqueue

Write an order and its invoice job in one transaction: the committed order's job runs, the rolled-back order leaves no job.

```bash
docker compose -f deploy/docker-compose.yml up -d --wait postgres  # Postgres on localhost:55432
go run ./examples/transactional-enqueue                            # safe to rerun
```

Default `FOREMAN_DATABASE_URL`:
`postgres://foreman:foreman@localhost:55432/foreman?sslmode=disable`.

## Expected output

```text
invoicing order 1
committed order: job 1 completed
rolled back order: no job
```
