// SPDX-License-Identifier: Apache-2.0

package elector

import (
	"context"
	"errors"
	"log/slog"
	"sync"
	"sync/atomic"
	"time"

	"github.com/thnhdatle/foreman/internal/domain/job"
	"github.com/thnhdatle/foreman/internal/platform/clock"
)

// Defaults.
const (
	DefaultName           = "default"
	DefaultTTL            = 15 * time.Second
	DefaultReleaseTimeout = 5 * time.Second
)

// Config configures an Elector.
type Config struct {
	Name     string
	ClientID string
	// TTL is the lease duration; the lease is renewed every TTL/3.
	TTL   time.Duration
	Store job.LeaseStore
	Clock clock.Clock
	// Services runs the leader-only services until its context is cancelled.
	Services func(ctx context.Context)
	// OnChange is called with true when leadership is gained and false when lost.
	OnChange       func(leader bool)
	Logger         *slog.Logger
	ReleaseTimeout time.Duration
}

// Elector holds a lease row to elect one leader per cluster. Clients that are
// not leader try to acquire the lease every TTL/3 (and on Wake); the leader
// renews it every TTL/3 and stops its services immediately when a renewal
// fails. On shutdown the lease is released so another client can
// take over at once.
type Elector struct {
	cfg     Config
	wake    chan struct{}
	crash   chan struct{}
	crashed sync.Once
	exited  chan struct{}
	started atomic.Bool
	leader  atomic.Bool
}

// New validates cfg and applies defaults.
func New(cfg Config) (*Elector, error) {
	if cfg.ClientID == "" || cfg.Store == nil || cfg.Clock == nil || cfg.Services == nil {
		return nil, errors.New("elector: ClientID, Store, Clock and Services are required")
	}
	if cfg.Name == "" {
		cfg.Name = DefaultName
	}
	if cfg.TTL <= 0 {
		cfg.TTL = DefaultTTL
	}
	if cfg.ReleaseTimeout <= 0 {
		cfg.ReleaseTimeout = DefaultReleaseTimeout
	}
	if cfg.Logger == nil {
		cfg.Logger = slog.New(slog.DiscardHandler)
	}
	return &Elector{
		cfg: cfg, wake: make(chan struct{}, 1), crash: make(chan struct{}), exited: make(chan struct{}),
	}, nil
}

// IsLeader reports whether this client currently holds the lease.
func (e *Elector) IsLeader() bool { return e.leader.Load() }

// Wake triggers an acquisition attempt now (another client resigned).
func (e *Elector) Wake() {
	select {
	case e.wake <- struct{}{}:
	default:
	}
}

// Interval is the renew and acquisition period, TTL/3.
func (e *Elector) Interval() time.Duration { return e.cfg.TTL / 3 }

// Run campaigns until ctx is cancelled, then resigns.
func (e *Elector) Run(ctx context.Context) error {
	e.started.Store(true)
	defer close(e.exited)
	var term *term
	for {
		term = e.step(ctx, term)
		select {
		case <-ctx.Done():
			e.stepDown(ctx, term, true)
			return nil
		case <-e.crash:
			e.stepDown(ctx, term, false)
			return nil
		case <-e.wake:
		case <-e.cfg.Clock.After(e.Interval()):
		}
	}
}

// term is one period of leadership: the running services and their cancel.
type term struct {
	cancel context.CancelFunc
	done   chan struct{}
}

func (e *Elector) step(ctx context.Context, t *term) *term {
	now := e.cfg.Clock.Now()
	// A lease call must finish well within the lease; the timeout is real time.
	callCtx, callCancel := context.WithTimeout(ctx, e.Interval())
	defer callCancel()
	if t != nil {
		ok, err := e.cfg.Store.RenewLease(callCtx, e.cfg.Name, e.cfg.ClientID, now, e.cfg.TTL)
		if err == nil && ok {
			return t
		}
		if ctx.Err() == nil {
			e.cfg.Logger.Warn("leadership lost", "error", errText(err))
		}
		e.stepDown(ctx, t, false)
		return nil
	}
	ok, err := e.cfg.Store.AcquireLease(callCtx, e.cfg.Name, e.cfg.ClientID, now, e.cfg.TTL)
	if err != nil {
		if ctx.Err() == nil {
			e.cfg.Logger.Warn("leader election attempt failed", "error", err.Error())
		}
		return nil
	}
	if !ok {
		return nil
	}
	sctx, cancel := context.WithCancel(ctx)
	t = &term{cancel: cancel, done: make(chan struct{})}
	e.leader.Store(true)
	e.cfg.Logger.Info("elected leader", "operation", "elect")
	if e.cfg.OnChange != nil {
		e.cfg.OnChange(true)
	}
	go func() {
		defer close(t.done)
		e.cfg.Services(sctx)
	}()
	return t
}

// stepDown stops leader services and, when release is true, resigns.
func (e *Elector) stepDown(ctx context.Context, t *term, release bool) {
	if t == nil {
		return
	}
	t.cancel()
	<-t.done
	e.leader.Store(false)
	if e.cfg.OnChange != nil {
		e.cfg.OnChange(false)
	}
	if !release {
		return
	}
	// Resigning happens during shutdown, after ctx is cancelled.
	rctx, cancel := context.WithTimeout(context.WithoutCancel(ctx), e.cfg.ReleaseTimeout)
	defer cancel()
	if err := e.cfg.Store.ReleaseLease(rctx, e.cfg.Name, e.cfg.ClientID); err != nil {
		e.cfg.Logger.Warn("resigning leadership failed; the lease will expire", "error", err.Error())
	}
}

// Crash simulates a leader crash for tests (crash seam): it stops
// the leader services immediately and abandons the lease without resigning,
// so other clients take over only after it expires. Run returns. Crash waits
// until Run has stopped; it is a no-op if Run was never started.
func (e *Elector) Crash() {
	e.crashed.Do(func() { close(e.crash) })
	if e.started.Load() {
		<-e.exited
	}
}

func errText(err error) string {
	if err == nil {
		return "lease taken by another client"
	}
	return err.Error()
}
