// SPDX-License-Identifier: Apache-2.0

package completer

import (
	"context"
	"errors"
	"log/slog"
	"time"

	"github.com/thnhdatle/foreman/internal/domain/job"
	"github.com/thnhdatle/foreman/internal/platform/clock"
)

const heartbeatChunk = 1000

type HeartbeatConfig struct {
	ClientID string
	Store    job.HeartbeatWriter
	Clock    clock.Clock
	Logger   *slog.Logger
	Interval time.Duration
	Running  func() []job.Ref
}

// Heartbeater refreshes the heartbeat of this client's running jobs every Interval.
type Heartbeater struct {
	cfg HeartbeatConfig
}

// NewHeartbeater validates cfg and applies defaults.
func NewHeartbeater(cfg HeartbeatConfig) (*Heartbeater, error) {
	if cfg.ClientID == "" || cfg.Store == nil || cfg.Clock == nil || cfg.Running == nil || cfg.Interval <= 0 {
		return nil, errors.New("completer: heartbeater needs ClientID, Store, Clock, Running and a positive Interval")
	}
	if cfg.Logger == nil {
		cfg.Logger = slog.New(slog.DiscardHandler)
	}
	return &Heartbeater{cfg: cfg}, nil
}

// Run sends heartbeats until ctx is cancelled.
func (h *Heartbeater) Run(ctx context.Context) error {
	for {
		select {
		case <-ctx.Done():
			return nil
		case <-h.cfg.Clock.After(h.cfg.Interval):
		}
		if err := h.beat(ctx, h.cfg.Running()); err != nil && ctx.Err() == nil {
			h.cfg.Logger.Warn("heartbeat failed", "error", err.Error())
		}
	}
}

// Beat refreshes the heartbeat of one job now.
func (h *Heartbeater) Beat(ctx context.Context, ref job.Ref) error {
	return h.beat(ctx, []job.Ref{ref})
}

func (h *Heartbeater) beat(ctx context.Context, refs []job.Ref) error {
	for len(refs) > 0 {
		n := min(len(refs), heartbeatChunk)
		callCtx, cancel := context.WithTimeout(ctx, h.cfg.Interval)
		_, err := h.cfg.Store.Heartbeat(callCtx, h.cfg.ClientID, h.cfg.Clock.Now(), refs[:n])
		cancel()
		if err != nil {
			return err
		}
		refs = refs[n:]
	}
	return nil
}
