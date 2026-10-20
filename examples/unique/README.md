# Unique jobs

Insert the same account sync twice with `UniqueOpts{ByArgFields: ["account_id"], ByPeriod: time.Hour}`; the second insert returns the first job.

```bash
docker compose -f deploy/docker-compose.yml up -d --wait postgres  # Postgres on localhost:55432
go run ./examples/unique                                           # safe to rerun
```

Account ids above 2^53 that differ in the last digit stay distinct.

Default `FOREMAN_DATABASE_URL`:
`postgres://foreman:foreman@localhost:55432/foreman?sslmode=disable`.

## Expected output

```text
second insert: duplicate skipped: true (job 1)
another account: duplicate skipped: false
```
