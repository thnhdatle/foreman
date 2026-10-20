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

// heartbeatChunk bounds the number of jobs per heartbeat statement.
const heartbeatChunk = 1000

// HeartbeatConfig configures a Heartbeater.
type HeartbeatConfig struct {
	ClientID string
	Store    job.HeartbeatWriter
	Clock    clock.Clock
	Logger   *slog.Logger
	// Interval between heartbeats (Config.HeartbeatInterval).
	Interval time.Duration
	// Running lists the attempts this client is executing.
	Running func() []job.Ref
}

// Heartbeater refreshes heartbeat_at of this client's running jobs in batches
// every Interval. The completion guard applies.
type Heartbeater struct {
	cfg HeartbeatConfig
}

// NewHeartbeater validates cfg.
func NewHeartbeater(cfg HeartbeatConfig) (*Heartbeater, error) {
	if cfg.ClientID == "" || cfg.Store == nil || cfg.Clock == nil || cfg.Running == nil || cfg.Interval <= 0 {
		return nil, errors.New("completer: heartbeater needs ClientID, Store, Clock, Running and a positive Interval")
	}
	if cfg.Logger == nil {
		cfg.Logger = slog.New(slog.DiscardHandler)
	}
	return &Heartbeater{cfg: cfg}, nil
}

// Run beats every Interval until ctx is cancelled.
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

// Beat refreshes the heartbeat of one attempt immediately (foreman.Heartbeat).
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
