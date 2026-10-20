# Acceptance report — foreman v0.1.0 — 2026-10-18
Environment: mac

| ID | Result | Measured | Threshold |
|---|---|---|---|
| FMN-01 | PASS | {"discarded":0,"elapsed_s":264.07034775,"executions":1000347,"finalized":1000000,"finalized_ratio":1,"jobs":1000000,"jobs_executed_more_than_once":347,"jobs_per_s":3786.869705441966,"jobs_rescued":351,"kills":5,"leader_kills":3,"lost":0,"mean_job_ms":48,"unexplained_duplicate_executions":0} | {"client_processes":4,"finalized_ratio_min":1,"heartbeat_interval_s":2,"jobs":1000000,"kill_interval_max_s":60,"kill_interval_min_s":20,"lost_max":0,"rescue_stuck_jobs_after_s":10,"unexplained_duplicate_executions_max":0,"workers_per_client":50} |
| FMN-02 | PASS | {"crashes":143,"duplicate_hourly":0,"duplicate_minute":0,"duplicate_ticks":0,"missed_hourly":0,"missed_minute":0,"missed_ticks":0,"real_seconds":130.944290042,"simulated_hours":24} | {"catch_up_min":5,"clients":3,"crash_every_simulated_min":10,"duplicate_ticks_max":0,"missed_ticks_max":0,"real_minutes_max":5,"simulated_hours":24} |
| FMN-03 | PASS | {"business_rows":5000,"committed":5000,"committed_jobs_ratio":1,"jobs_from_committed":5000,"jobs_from_rolled_back":0,"rolled_back":5000,"transactions":10000} | {"committed_jobs_ratio_min":1,"jobs_from_rolled_back_max":0,"rolled_back_ratio":0.5,"transactions":10000} |
| FMN-04 | PASS | {"completed":10435674,"duration_s":300,"fair_completed":1913889,"fair_keys":1000,"fair_per_key_limit":50,"fair_throughput_jobs_per_s":6379.627460568024,"throughput_jobs_per_s":34785.54264426956,"top_ups":42} | {"clients":4,"duration_s":300,"fair_keys":1000,"fair_per_key_limit":50,"prefill_jobs":1000000,"throughput_min_jobs_per_s":5000,"workers_per_client":50} |
| FMN-05 | PASS | {"duration_s":120,"inserted":129654,"measured_jobs":119704,"p50_ms":5.071,"p99_ms":8.881,"rate_per_s":997.3384615384615} | {"clients":4,"duration_s":120,"insert_rate_per_s":1000,"p99_ms_max":100,"warmup_s":10,"workers_per_client":50} |
| FMN-06 | PASS | {"b_p95_alone_ms":2.659,"b_p95_fair_ms":41.005,"b_p95_unfair_ms":64576.228,"backlog_jobs":240000,"capacity_jobs_per_s":4000,"limit_vs_alone_ms":100,"limit_vs_unfair_ms":12915.245600000002} | {"a_rate_factor":0.8,"b_rate_factor":0.04,"backlog_capacity_seconds":60,"clients":4,"duration_s":120,"fair_floor_ms":100,"fair_vs_alone_factor_max":2,"fair_vs_unfair_factor_max":0.2,"job_ms":50,"workers_per_client":50} |
| FMN-07 | PASS | {"clients":4,"completed":100000,"db_samples":7637,"db_violations":0,"elapsed_s":76.455964958,"jobs":100000,"jobs_per_s":1307.942422215632,"keys":1000,"max_running_per_key":5,"per_key_limit":5,"violations":0,"worker_violations":0} | {"clients":4,"jobs":100000,"keys":1000,"per_key_limit":5,"sample_interval_ms":10,"violations_max":0,"workers_per_client":50} |
| FMN-08 | PASS | {"distinct_identities":100,"elapsed_s":0.69529,"goroutines":50,"inserts":10000,"jobs":100,"processes":2,"unique_keys":100} | {"goroutines":50,"inserts":10000,"jobs_exact":100,"processes":2,"sql_function_ratio":0.5,"unique_keys":100} |
| FMN-09 | PASS | {"completed_by":"b","completed_by_other_client":true,"final_attempt":2,"frozen_client":"a","rescue_bound_s":14,"rescue_error":"rescued: no heartbeat since 2026-10-18T00:11:23.713889Z","rescue_latency_s":11.921342,"stale_completion_rejected":true} | {"completed_by_other_client":true,"heartbeat_interval_s":2,"rescue_stuck_jobs_after_s":10,"rescue_within_heartbeats":2,"stale_completion_rejected":true} |
| FMN-10 | PASS | {"clients":2,"clients_recovered":2,"completed":14000,"completion_mismatch":0,"executions":14000,"jobs":14000,"jobs_run":14000,"lost":0,"outage_s":30.001254084,"ran_after_restore":{"client-0":5064,"client-1":4956},"rescued":0} | {"clients":2,"completion_mismatch_max":0,"load_after_s":20,"load_before_s":20,"load_jobs_per_s":200,"lost_max":0,"outage_s":30,"rescue_stuck_jobs_after_s":120} |
| FMN-11 | PASS | {"cancel_latency_max_ms":16.23,"cancel_latency_p50_ms":0.656,"cancel_latency_p99_ms":9.842,"cancelled":1000,"cancelled_ratio":1,"jobs":1000,"observed":1000} | {"cancel_latency_p99_ms_max":1000,"cancelled_ratio_min":1,"clients":2,"jobs":1000} |
| FMN-12 | PASS | {"duration_s":3600,"inserted":3595102,"p99_drift":0.006599670867417044,"p99_end_ms":8.516867999999999,"p99_half_ms":8.461027999999999,"rate_per_s":998.6394444444444,"rows_end":300501,"rows_half":300198,"size_drift":0.00007736345350456444,"size_end_bytes":105889792,"size_half_bytes":105897984} | {"cleaner_interval_s":1,"completed_retention_s":300,"duration_s":3600,"p99_drift_max":0.5,"rate_per_s":1000,"reindex_interval_s":600,"size_drift_max":0.2} |
| FMN-13 | PASS | {"duration_s":2.96397225,"job_completed":true,"success":true} | {"duration_max_s":300,"success":true} |

## Not met
- none

## Skipped (environment)
- none

## Blocked (environment)
- none

## Sibling images used
- none (Foreman depends on no sibling project)
