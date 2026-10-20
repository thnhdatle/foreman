# Periodic jobs

Run a `PeriodicJob` every second (`foreman.Every`); the leader enqueues each tick once and the example stops after three runs.

```bash
docker compose -f deploy/docker-compose.yml up -d --wait postgres  # Postgres on localhost:55432
go run ./examples/periodic                                         # safe to rerun
```

Default `FOREMAN_DATABASE_URL`:
`postgres://foreman:foreman@localhost:55432/foreman?sslmode=disable`.

## Expected output

```text
periodic job ran (1)
periodic job ran (2)
periodic job ran (3)
```
