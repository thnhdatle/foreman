// SPDX-License-Identifier: Apache-2.0

package foreman

import (
	"crypto/rand"
	"encoding/hex"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"regexp"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"go.opentelemetry.io/otel/trace"

	"github.com/thnhdatle/foreman/internal/platform/clock"
)

// Clock is the time source of a client. Every time Foreman writes or
// compares in SQL comes from it. foremantest provides a fake.
type Clock = clock.Clock

// Ticker is a ticker created by a Clock.
type Ticker = clock.Ticker

// Defaults of Config.
const (
	DefaultMaxWorkers            = 10
	DefaultFetchInterval         = time.Second
	DefaultHeartbeatInterval     = 30 * time.Second
	DefaultRescueStuckJobsAfter  = time.Hour
	DefaultCompletedJobRetention = 24 * time.Hour
	DefaultCancelledJobRetention = 7 * 24 * time.Hour
	DefaultDiscardedJobRetention = 7 * 24 * time.Hour
	DefaultReindexInterval       = 24 * time.Hour
	DefaultCleanerInterval       = 30 * time.Second
	DefaultMaxArgsBytes          = 1 << 20
	DefaultSchema                = "public"
)

// Config configures a client. The zero value of every field selects its
// default; durations that can be switched off use a negative value for "off"
// (Go zero values cannot mean both "default" and "off").
type Config struct {
	// Queues served by this client. With no queues the client is
	// producer-only: it inserts and administers but fetches nothing and
	// takes no part in leader election.
	Queues map[string]QueueConfig
	// Workers registered for the kinds this client runs.
	Workers *Workers
	// PeriodicJobs are enqueued by the leader on their schedule.
	PeriodicJobs []*PeriodicJob
	// Logger defaults to slog.Default().
	Logger *slog.Logger
	// Clock defaults to the wall clock.
	Clock Clock
	// ID identifies the client in attempted_by and the leader lease;
	// default hostname-pid-random.
	ID string
	// Schema holding the Foreman tables; default "public".
	Schema string
	// JobTimeout is the default timeout of Work; 0 = none.
	JobTimeout time.Duration
	// RescueStuckJobsAfter returns running jobs without a heartbeat for this
	// long to retryable; default 1h, at least 2 x HeartbeatInterval.
	RescueStuckJobsAfter time.Duration
	// HeartbeatInterval between heartbeats of running jobs; default 30s.
	HeartbeatInterval time.Duration
	// RetryPolicy defaults to ExponentialBackoff{} (1s base, 24h cap, full jitter).
	RetryPolicy RetryPolicy
	// CompletedJobRetention defaults to 24h; negative keeps completed jobs forever.
	CompletedJobRetention time.Duration
	// CancelledJobRetention defaults to 7 days; negative keeps them forever.
	CancelledJobRetention time.Duration
	// DiscardedJobRetention defaults to 7 days; negative keeps them forever.
	DiscardedJobRetention time.Duration
	// ReindexInterval rebuilds the queue indexes concurrently; default 24h,
	// negative disables it.
	ReindexInterval time.Duration
	// CleanerInterval between runs of the cleaner; default 30s.
	CleanerInterval time.Duration
	// ErrorHandler may override the outcome of failed attempts.
	ErrorHandler ErrorHandler
	// Middleware wraps inserts and work, outermost first.
	Middleware []Middleware
	// PollOnly disables LISTEN/NOTIFY (for PgBouncer transaction pooling):
	// wake-ups, cancels and pauses then propagate by polling every FetchInterval.
	PollOnly bool
	// MaxArgsBytes bounds the JSON size of args; default 1 MiB.
	MaxArgsBytes int
	// TracerProvider enables OpenTelemetry spans "foreman.work <kind>".
	TracerProvider trace.TracerProvider
	// Registerer receives the Prometheus metrics; nil keeps them private.
	Registerer prometheus.Registerer
}

var nameRe = regexp.MustCompile(`^[A-Za-z0-9][A-Za-z0-9_.:\-]{0,127}$`)

// normalized returns a copy of c with defaults applied, or every validation
// problem joined under ErrInvalidConfig.
func (c Config) normalized() (Config, error) {
	var errs []error
	if c.Workers == nil {
		c.Workers = NewWorkers()
	}
	if c.Logger == nil {
		c.Logger = slog.Default()
	}
	if c.Clock == nil {
		c.Clock = clock.New()
	}
	if c.ID == "" {
		c.ID = defaultClientID()
	}
	if c.Schema == "" {
		c.Schema = DefaultSchema
	}
	if c.RetryPolicy == nil {
		c.RetryPolicy = ExponentialBackoff{}
	}
	setDuration(&c.HeartbeatInterval, DefaultHeartbeatInterval)
	setDuration(&c.RescueStuckJobsAfter, DefaultRescueStuckJobsAfter)
	setDuration(&c.CompletedJobRetention, DefaultCompletedJobRetention)
	setDuration(&c.CancelledJobRetention, DefaultCancelledJobRetention)
	setDuration(&c.DiscardedJobRetention, DefaultDiscardedJobRetention)
	setDuration(&c.ReindexInterval, DefaultReindexInterval)
	setDuration(&c.CleanerInterval, DefaultCleanerInterval)
	if c.MaxArgsBytes == 0 {
		c.MaxArgsBytes = DefaultMaxArgsBytes
	}

	if c.HeartbeatInterval < 0 || c.CleanerInterval < 0 || c.JobTimeout < 0 || c.MaxArgsBytes < 0 {
		errs = append(errs, errors.New(
			"HeartbeatInterval, CleanerInterval, JobTimeout and MaxArgsBytes must not be negative"))
	}
	if c.RescueStuckJobsAfter < 2*c.HeartbeatInterval {
		errs = append(errs, fmt.Errorf("RescueStuckJobsAfter %s must be at least 2 x HeartbeatInterval (%s)",
			c.RescueStuckJobsAfter, c.HeartbeatInterval))
	}
	if len(c.ID) > 200 {
		errs = append(errs, errors.New("ID must be at most 200 characters"))
	}
	var qerrs []error
	c.Queues, qerrs = normalizedQueues(c.Queues)
	errs = append(errs, qerrs...)
	errs = append(errs, validatePeriodic(c.PeriodicJobs)...)
	if len(errs) > 0 {
		return c, fmt.Errorf("%w: %w", ErrInvalidConfig, errors.Join(errs...))
	}
	return c, nil
}

func normalizedQueues(in map[string]QueueConfig) (map[string]QueueConfig, []error) {
	var errs []error
	out := make(map[string]QueueConfig, len(in))
	for name, q := range in {
		if !nameRe.MatchString(name) {
			errs = append(errs, fmt.Errorf("queue name %q must match %s", name, nameRe))
		}
		if q.MaxWorkers < 0 || q.FetchInterval < 0 || q.PerKeyLimit < 0 {
			errs = append(errs, fmt.Errorf("queue %q: MaxWorkers, FetchInterval and PerKeyLimit must not be negative", name))
		}
		if q.MaxWorkers == 0 {
			q.MaxWorkers = DefaultMaxWorkers
		}
		if q.FetchInterval == 0 {
			q.FetchInterval = DefaultFetchInterval
		}
		out[name] = q
	}
	return out, errs
}

func setDuration(d *time.Duration, def time.Duration) {
	if *d == 0 {
		*d = def
	}
}

func defaultClientID() string {
	host, err := os.Hostname()
	if err != nil || host == "" {
		host = "foreman"
	}
	b := make([]byte, 4)
	_, _ = rand.Read(b)
	return fmt.Sprintf("%s-%d-%s", host, os.Getpid(), hex.EncodeToString(b))
}

// minFetchInterval returns the smallest fetch interval among queues (default 1s).
func (c Config) minFetchInterval() time.Duration {
	m := DefaultFetchInterval
	for _, q := range c.Queues {
		if q.FetchInterval > 0 && q.FetchInterval < m {
			m = q.FetchInterval
		}
	}
	return m
}
