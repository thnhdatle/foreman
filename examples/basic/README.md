# Basic usage

Enqueue an email in the user row's transaction (`InsertTx`) and wait for it. The README example.

```bash
docker compose -f deploy/docker-compose.yml up -d --wait postgres  # Postgres on localhost:55432
go run ./examples/basic                                            # safe to rerun
```

Default `FOREMAN_DATABASE_URL`:
`postgres://foreman:foreman@localhost:55432/foreman?sslmode=disable`.

## Expected output

```text
sending "welcome" email to user 1 (attempt 1)
job completed: id=1 state=completed
```
