# Acceptance suite

Run Foreman's end-to-end guarantee checks against a real Postgres. They produce the numbers in
the README and [`bench/RESULTS.md`](../bench/RESULTS.md).

## Running

```sh
make acceptance                    # all criteria, reference-machine thresholds
make acceptance CI=1               # CI thresholds: smaller loads, 10-minute soak
make acceptance ONLY=FMN-01,FMN-07 # a subset
```

The runner starts the [compose stack](../deploy/docker-compose.yml) (ports 55432, 55433, 58474),
writes `acceptance/report.md`, then stops it.

Needs Docker with 8+ CPUs and 20 GB for the VM, and Go 1.27. A full run takes 1.5 to 2 hours
(FMN-12 alone: one hour). Reference machine: MacBook Pro M3 Pro, 36 GB (`machine` in
`criteria.yaml`).

| Variable (optional) | Use |
|---|---|
| `FOREMAN_ACC_DATABASE_URL` | existing Postgres |
| `FOREMAN_ACC_PROXY_DATABASE_URL` | existing Postgres via Toxiproxy |
| `FOREMAN_ACC_TOXIPROXY_URL` | existing Toxiproxy |
| `CI=1` | CI thresholds |

## Criteria

Thresholds are in [`criteria.yaml`](criteria.yaml): `pass.mac` (reference machine), `pass.ci`
(CI). Tests are named after the ID (`TestAcceptance_FMN01_…` or `BenchmarkAcceptance_…`).

| ID | Kind | Checks |
|---|---|---|
| FMN-01 | functional | no lost jobs under `kill -9` of workers and leader |
| FMN-02 | functional | periodic tick enqueued once across leader crashes |
| FMN-03 | functional | rollback leaves no jobs |
| FMN-04 | performance | no-op throughput, 4 clients × 50 workers |
| FMN-05 | performance | pickup latency at 1,000 inserts/s |
| FMN-06 | performance | small tenant's wait under overload |
| FMN-07 | functional | per-key limit never exceeded across clients |
| FMN-08 | functional | Go and SQL unique inserts: one job per key |
| FMN-09 | functional | frozen worker's job rescued, stale completion rejected |
| FMN-10 | functional | recovery from 30 s connection loss, no lost results |
| FMN-11 | functional | cancel reaches the job context within 1 s |
| FMN-12 | performance | table size and latency stable over one-hour soak |
| FMN-13 | release | README quickstart works from a clean checkout |

Off the reference machine, performance criteria use the CI thresholds. The report says so.

## The report

`report.md` has one row per criterion: result, measured, threshold. Logs go to
`acceptance/logs/<ID>.txt` and `.json` (not committed).

| Result | Meaning |
|---|---|
| **PASS** | every value meets its threshold |
| **FAIL** | a value misses; also listed under "Not met" |
| **Skipped (environment)** | a required capability (e.g. Docker) is missing; the runner decides |
| **Blocked (environment)** | reference-machine thresholds cannot apply on this host |

The run fails unless each criterion has exactly one test whose name matches `criteria.yaml`.
