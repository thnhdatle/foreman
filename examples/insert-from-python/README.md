# Insert from Python

Enqueue a job from Python with the SQL function `foreman_insert_job(kind, args, opts)` and run it in a Go worker.

```bash
docker compose -f deploy/docker-compose.yml up -d --wait postgres  # Postgres on localhost:55432
go run ./examples/insert-from-python                               # safe to rerun
```

Requires `uv`. The first run fetches Python 3.12 and psycopg. The script inserts one unique job twice and gets one id back.

Default `FOREMAN_DATABASE_URL`:
`postgres://foreman:foreman@localhost:55432/foreman?sslmode=disable`.

## Expected output

```text
python inserted job 1; unique duplicate returned job 1
go worker sending "welcome" to ada@example.com
job 1 completed
```
