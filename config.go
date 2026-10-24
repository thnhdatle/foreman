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

// Clock is the time source of a client.
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

// Config configures a client. Zero fields select their defaults.
type Config struct {
	// Queues served by this client. With none it is producer-only.
	Queues       map[string]QueueConfig
	Workers      *Workers
	PeriodicJobs []*PeriodicJob
	Logger       *slog.Logger
	Clock        Clock
	// ID identifies the client. Default hostname-pid-random.
	ID string
	// Schema holding the Foreman tables. Default public.
	Schema string
	// JobTimeout is the default timeout of Work. 0 means none.
	JobTimeout time.Duration
	// RescueStuckJobsAfter is how long a running job may go without a heartbeat. Default 1h.
	RescueStuckJobsAfter time.Duration
	// HeartbeatInterval is the time between heartbeats. Default 30s.
	HeartbeatInterval time.Duration
	// RetryPolicy defaults to ExponentialBackoff{}.
	RetryPolicy RetryPolicy
	// CompletedJobRetention defaults to 24h. Negative keeps jobs forever.
	CompletedJobRetention time.Duration
	// CancelledJobRetention defaults to 7 days. Negative keeps jobs forever.
	CancelledJobRetention time.Duration
	// DiscardedJobRetention defaults to 7 days. Negative keeps jobs forever.
	DiscardedJobRetention time.Duration
	// ReindexInterval defaults to 24h. Negative disables it.
	ReindexInterval time.Duration
	// CleanerInterval defaults to 30s.
	CleanerInterval time.Duration
	ErrorHandler    ErrorHandler
	Middleware      []Middleware
	// PollOnly disables LISTEN/NOTIFY and relies on polling.
	PollOnly bool
	// MaxArgsBytes bounds the JSON size of args. Default 1 MiB.
	MaxArgsBytes   int
	TracerProvider trace.TracerProvider
	Registerer     prometheus.Registerer
}

var nameRe = regexp.MustCompile(`^[A-Za-z0-9][A-Za-z0-9_.:\-]{0,127}$`)

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

func (c Config) minFetchInterval() time.Duration {
	m := DefaultFetchInterval
	for _, q := range c.Queues {
		if q.FetchInterval > 0 && q.FetchInterval < m {
			m = q.FetchInterval
		}
	}
	return m
}
