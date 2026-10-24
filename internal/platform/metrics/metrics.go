// SPDX-License-Identifier: Apache-2.0

package metrics

import (
	"errors"
	"runtime"
	"runtime/debug"
	"sync"
	"time"

	"github.com/prometheus/client_golang/prometheus"
)

const DefaultLabelCap = 50

const Other = "other"

// Metrics holds the Prometheus collectors of a client.
type Metrics struct {
	Inserted        *prometheus.CounterVec
	Completed       *prometheus.CounterVec
	Duration        *prometheus.HistogramVec
	QueueLatency    *prometheus.HistogramVec
	QueueDepth      *prometheus.GaugeVec
	Running         *prometheus.GaugeVec
	Leader          prometheus.Gauge
	Rescued         prometheus.Counter
	Stale           prometheus.Counter
	PeriodicSkipped *prometheus.CounterVec
	Unserved        *prometheus.CounterVec
	FetchDuration   *prometheus.HistogramVec
	MaintenanceRun  *prometheus.GaugeVec
	MaintenanceRows *prometheus.CounterVec
	BuildInfo       *prometheus.GaugeVec

	queues *Capper
	kinds  *Capper
	names  *Capper
}

// New creates the collectors and registers them with reg.
func New(reg prometheus.Registerer) (*Metrics, error) {
	if reg == nil {
		reg = prometheus.NewRegistry()
	}
	m := &Metrics{queues: NewCapper(DefaultLabelCap), kinds: NewCapper(DefaultLabelCap), names: NewCapper(DefaultLabelCap)}
	var errs []error
	m.Inserted = register(reg, prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: "foreman_jobs_inserted_total", Help: "Jobs inserted by this client.",
	}, []string{"queue", "kind"}), &errs)
	m.Completed = register(reg, prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: "foreman_jobs_completed_total", Help: "Attempts finished, by outcome.",
	}, []string{"queue", "kind", "outcome"}), &errs)
	m.Duration = register(reg, prometheus.NewHistogramVec(prometheus.HistogramOpts{
		Name: "foreman_job_duration_seconds", Help: "Run time of Work.", Buckets: DurationBuckets,
	}, []string{"queue", "kind"}), &errs)
	m.QueueLatency = register(reg, prometheus.NewHistogramVec(prometheus.HistogramOpts{
		Name: "foreman_job_queue_latency_seconds", Help: "Time from scheduled_at to attempted_at.",
		Buckets: LatencyBuckets,
	}, []string{"queue"}), &errs)
	m.QueueDepth = register(reg, prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Name: "foreman_queue_depth", Help: "Pending jobs per queue and state, sampled by the leader.",
	}, []string{"queue", "state"}), &errs)
	m.Running = register(reg, prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Name: "foreman_running", Help: "Jobs running in this client.",
	}, []string{"queue"}), &errs)
	m.Leader = register(reg, prometheus.NewGauge(prometheus.GaugeOpts{
		Name: "foreman_leader", Help: "1 when this client is the leader.",
	}), &errs)
	m.Rescued = register(reg, prometheus.NewCounter(prometheus.CounterOpts{
		Name: "foreman_rescued_total", Help: "Running jobs rescued after their heartbeat stopped.",
	}), &errs)
	m.Stale = register(reg, prometheus.NewCounter(prometheus.CounterOpts{
		Name: "foreman_stale_completions_total", Help: "Completions rejected by the completion guard.",
	}), &errs)
	m.PeriodicSkipped = register(reg, prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: "foreman_periodic_skipped_total", Help: "Periodic ticks skipped beyond the catch-up window.",
	}, []string{"name"}), &errs)
	m.Unserved = register(reg, prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: "foreman_jobs_unserved_queue_total", Help: "Jobs inserted into a queue no running client serves.",
	}, []string{"queue"}), &errs)
	m.FetchDuration = register(reg, prometheus.NewHistogramVec(prometheus.HistogramOpts{
		Name: "foreman_fetch_duration_seconds", Help: "Duration of fetch rounds.", Buckets: FetchBuckets,
	}, []string{"queue"}), &errs)
	m.MaintenanceRun = register(reg, prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Name: "foreman_maintenance_last_run_timestamp_seconds", Help: "Last run of a leader service.",
	}, []string{"service"}), &errs)
	m.MaintenanceRows = register(reg, prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: "foreman_maintenance_rows_total", Help: "Rows affected by a leader service.",
	}, []string{"service"}), &errs)
	m.BuildInfo = register(reg, prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Name: "foreman_build_info", Help: "Foreman module version.",
	}, []string{"version", "go_version"}), &errs)
	if err := errors.Join(errs...); err != nil {
		return nil, err
	}
	m.BuildInfo.WithLabelValues(moduleVersion(), runtime.Version()).Set(1)
	return m, nil
}

func register[C prometheus.Collector](reg prometheus.Registerer, c C, errs *[]error) C {
	err := reg.Register(c)
	if err == nil {
		return c
	}
	var are prometheus.AlreadyRegisteredError
	if errors.As(err, &are) {
		if existing, ok := are.ExistingCollector.(C); ok {
			return existing
		}
	}
	*errs = append(*errs, err)
	return c
}

func moduleVersion() string {
	info, ok := debug.ReadBuildInfo()
	if !ok {
		return "unknown"
	}
	for _, dep := range info.Deps {
		if dep.Path == "github.com/thnhdatle/foreman" {
			return dep.Version
		}
	}
	if info.Main.Path == "github.com/thnhdatle/foreman" && info.Main.Version != "" {
		return info.Main.Version
	}
	return "devel"
}

func (m *Metrics) Queue(q string) string { return m.queues.Value(q) }

func (m *Metrics) Kind(k string) string { return m.kinds.Value(k) }

func (m *Metrics) Name(n string) string { return m.names.Value(n) }

func (m *Metrics) ObserveMaintenance(service string, rows int, at time.Time) {
	m.MaintenanceRun.WithLabelValues(service).Set(float64(at.UnixNano()) / 1e9)
	m.MaintenanceRows.WithLabelValues(service).Add(float64(rows))
}

type Capper struct {
	max  int
	mu   sync.Mutex
	seen map[string]struct{}
}

func NewCapper(maxValues int) *Capper {
	return &Capper{max: maxValues, seen: map[string]struct{}{}}
}

func (c *Capper) Value(v string) string {
	c.mu.Lock()
	defer c.mu.Unlock()
	if _, ok := c.seen[v]; ok {
		return v
	}
	if len(c.seen) >= c.max {
		return Other
	}
	c.seen[v] = struct{}{}
	return v
}
