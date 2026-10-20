# Benchmarks

Run the benchmarks and regenerate [RESULTS.md](RESULTS.md).

```sh
make bench                                       # micro benchmarks (6 counts), then each criterion 3 times
go tool benchstat out/bench/micro.txt            # compare micro benchmark runs
go run ./bench/cmd/results -only FMN-05 -runs 1  # one scenario
```

Micro benchmarks run `go test -bench . ./internal/...`. RESULTS.md gets the median and min-max
of each measurement. Raw JSON and logs go to `out/bench/` (gitignored). CI runs each criterion
once with CI thresholds (`make acceptance CI=1`).

Reference machine: MacBook Pro M3 Pro 36 GB, Docker VM 8 CPU / 20 GB.

| Scenario | Criterion | What runs |
|---|---|---|
| Throughput | FMN-04 | 1M no-op jobs pre-filled and topped up, 4 clients x 50 workers, 5 minutes; then the same with fairness and a per-key limit of 50 over 1,000 keys |
| Pickup latency | FMN-05 | 1,000 single-job inserts per second for 2 minutes (after 10 s warm-up); `attempted_at - created_at` |
| Fairness | FMN-06 | 50 ms jobs, 4 x 50 workers (C = 4,000 jobs/s); 240,000 backlog of tenant A, then A at 0.8 C and B at 0.04 C (Poisson) for 2 minutes; fair, unfair and B alone |
| Soak | FMN-12 | 1 hour at 1,000 jobs/s, 5-minute retention, cleaner every second, reindex every 10 minutes |

Clients run natively in the test process. Only Postgres 17 runs in Docker
(`deploy/docker-compose.yml` holds its settings). Load is open model: inserts follow a fixed rate
and never wait for workers.
