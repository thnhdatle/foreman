# Multi-tenant fairness

Serve a small tenant's 5 jobs before a big tenant's 200-job backlog drains, with `Fairness: true` and `PerKeyLimit: 3`.

```bash
docker compose -f deploy/docker-compose.yml up -d --wait postgres  # Postgres on localhost:55432
go run ./examples/multi-tenant-fairness                            # safe to rerun
```

Default `FOREMAN_DATABASE_URL`:
`postgres://foreman:foreman@localhost:55432/foreman?sslmode=disable`.

## Expected output

```text
small tenant served before the backlog drained: true
```
