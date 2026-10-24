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

const (
	DefaultName           = "default"
	DefaultTTL            = 15 * time.Second
	DefaultReleaseTimeout = 5 * time.Second
)

type Config struct {
	Name           string
	ClientID       string
	TTL            time.Duration
	Store          job.LeaseStore
	Clock          clock.Clock
	Services       func(ctx context.Context)
	OnChange       func(leader bool)
	Logger         *slog.Logger
	ReleaseTimeout time.Duration
}

// Elector holds a lease to elect one leader and runs the leader services while it holds it.
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

func (e *Elector) IsLeader() bool { return e.leader.Load() }

func (e *Elector) Wake() {
	select {
	case e.wake <- struct{}{}:
	default:
	}
}

func (e *Elector) Interval() time.Duration { return e.cfg.TTL / 3 }

// Run competes for the lease until ctx is cancelled, then releases it.
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

type term struct {
	cancel context.CancelFunc
	done   chan struct{}
}

func (e *Elector) step(ctx context.Context, t *term) *term {
	now := e.cfg.Clock.Now()
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
	rctx, cancel := context.WithTimeout(context.WithoutCancel(ctx), e.cfg.ReleaseTimeout)
	defer cancel()
	if err := e.cfg.Store.ReleaseLease(rctx, e.cfg.Name, e.cfg.ClientID); err != nil {
		e.cfg.Logger.Warn("resigning leadership failed; the lease will expire", "error", err.Error())
	}
}

// Crash stops the leader services and drops the lease without releasing it.
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
