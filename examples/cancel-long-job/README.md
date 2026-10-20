# Cancel a long job

Cancel a running export with `JobCancel`: its context ends with cause `job: cancel requested` and the job ends `cancelled`.

```bash
docker compose -f deploy/docker-compose.yml up -d --wait postgres  # Postgres on localhost:55432
go run ./examples/cancel-long-job                                  # safe to rerun
```

Default `FOREMAN_DATABASE_URL`:
`postgres://foreman:foreman@localhost:55432/foreman?sslmode=disable`.

## Expected output

```text
export stopped after
job cancelled: state=cancelled
```
