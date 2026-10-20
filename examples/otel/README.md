# OpenTelemetry tracing

Set `Config.TracerProvider` and print the `foreman.insert <kind>` and `foreman.work <kind>` spans with their job attributes.

```bash
docker compose -f deploy/docker-compose.yml up -d --wait postgres  # Postgres on localhost:55432
go run ./examples/otel                                             # safe to rerun
```

In production, use an OTLP exporter.

Default `FOREMAN_DATABASE_URL`:
`postgres://foreman:foreman@localhost:55432/foreman?sslmode=disable`.

## Expected output

```text
span: foreman.insert resize
span: foreman.work resize
foreman.job.attempt=1
```
