# Roadmap

Features that are not in v0.1 and planned follow-ups. Items marked `[gfi]` are suitable as good
first issues.

## Not in v0.1

- Workflow DAGs (jobs that depend on other jobs).
- Step checkpoints / resumable jobs.
- Web UI for the admin API (`foremanhttp` serves JSON only).
- Python and Ruby SDKs (v0.1 documents the `foreman_insert_job` SQL contract instead).
- Rate limiting per key (v0.1 has per-key concurrency limits only).
- Job batches with completion callbacks.
- Sequences (strictly ordered jobs per key).
- `database/sql` driver support (v0.1 is pgx-only).
- SQLite backend.
- Multi-database sharding.

## Follow-ups

- OpenTelemetry `MeterProvider` support next to the Prometheus `Registerer`.
- A dead-letter metric: jobs discarded because their attempts ran out are counted today only as
  `outcome="error"` of their last attempt. `[gfi]`
- Configurable leader lease TTL (fixed at 15 s).
- Cancel the context of a running job whose heartbeat is rejected (the job was rescued or
  discarded), instead of letting it run to a stale completion.
- Periodic clock-skew checks (today only at `Start`), exported as a metric. `[gfi]`
- Cache the key list between keyed fetch rounds for queues with thousands of concurrency keys.
- Vixie-cron DST semantics as an option (run once in a repeated hour).
- `foreman_insert_job`: apply the Go-side checks too (name pattern, args size, metadata must be an
  object, past `scheduled_at` replaced with now). `[gfi]`
- CLI exit code 2 for usage errors (unknown flags or commands exit 1 today). `[gfi]`
- Helm chart and Kubernetes manifests for the CLI as a Job (migrations).
