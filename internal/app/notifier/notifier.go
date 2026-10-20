// SPDX-License-Identifier: Apache-2.0

package notifier

import (
	"context"
	"errors"
	"log/slog"
	"math/rand/v2"
	"sync"
	"sync/atomic"
	"time"

	"github.com/thnhdatle/foreman/internal/domain/job"
	"github.com/thnhdatle/foreman/internal/platform/clock"
)

// Defaults for reconnect backoff.
const (
	DefaultMinBackoff = 100 * time.Millisecond
	DefaultMaxBackoff = 5 * time.Second
	// DefaultIdleTimeout is how long the connection may stay quiet before it
	// is pinged; a connection that died silently is found within twice this.
	DefaultIdleTimeout = 30 * time.Second
	closeTimeout       = 5 * time.Second
)

// Config configures a Notifier.
type Config struct {
	Listener job.Listener
	Clock    clock.Clock
	Logger   *slog.Logger
	// MinBackoff and MaxBackoff bound the jittered reconnect delay.
	MinBackoff time.Duration
	MaxBackoff time.Duration
	// IdleTimeout is how long to wait for a notification before pinging the
	// connection.
	IdleTimeout time.Duration
	// OnConnect runs after every successful (re)connection; notifications
	// sent while disconnected are lost, so callers re-check state here.
	OnConnect func()
	Rand      func() float64
}

// Notifier multiplexes one LISTEN connection to many subscribers and
// reconnects with jittered exponential backoff when the connection drops.
type Notifier struct {
	cfg       Config
	connected atomic.Bool

	mu   sync.RWMutex // protects subs
	subs map[string][]func(payload string)
}

// New validates cfg.
func New(cfg Config) (*Notifier, error) {
	if cfg.Listener == nil || cfg.Clock == nil {
		return nil, errors.New("notifier: Listener and Clock are required")
	}
	if cfg.MinBackoff <= 0 {
		cfg.MinBackoff = DefaultMinBackoff
	}
	if cfg.MaxBackoff < cfg.MinBackoff {
		cfg.MaxBackoff = max(DefaultMaxBackoff, cfg.MinBackoff)
	}
	if cfg.IdleTimeout <= 0 {
		cfg.IdleTimeout = DefaultIdleTimeout
	}
	if cfg.Logger == nil {
		cfg.Logger = slog.New(slog.DiscardHandler)
	}
	if cfg.Rand == nil {
		cfg.Rand = rand.Float64
	}
	return &Notifier{cfg: cfg, subs: map[string][]func(string){}}, nil
}

// Subscribe registers fn for channel. fn runs on the notifier goroutine and
// must not block. Subscriptions must be made before Run.
func (n *Notifier) Subscribe(channel string, fn func(payload string)) {
	n.mu.Lock()
	defer n.mu.Unlock()
	n.subs[channel] = append(n.subs[channel], fn)
}

// Connected reports whether the LISTEN connection is up.
func (n *Notifier) Connected() bool { return n.connected.Load() }

// Run listens until ctx is cancelled, reconnecting after failures.
func (n *Notifier) Run(ctx context.Context) error {
	failures := 0
	for {
		established, err := n.session(ctx)
		n.connected.Store(false)
		if ctx.Err() != nil {
			return nil //nolint:nilerr // the session error is the cancellation itself
		}
		if established {
			failures = 0
		}
		failures++
		n.cfg.Logger.Warn("notifier connection lost, reconnecting", "error", err.Error(), "attempt", failures)
		select {
		case <-ctx.Done():
			return nil
		case <-n.cfg.Clock.After(n.backoff(failures)):
		}
	}
}

func (n *Notifier) backoff(failures int) time.Duration {
	d := n.cfg.MinBackoff << min(failures-1, 20)
	if d <= 0 || d > n.cfg.MaxBackoff {
		d = n.cfg.MaxBackoff
	}
	return d/2 + time.Duration(n.cfg.Rand()*float64(d/2))
}

func (n *Notifier) channels() []string {
	n.mu.RLock()
	defer n.mu.RUnlock()
	out := make([]string, 0, len(n.subs))
	for ch := range n.subs {
		out = append(out, ch)
	}
	return out
}

// session runs one connection; established reports whether LISTEN succeeded.
func (n *Notifier) session(ctx context.Context) (established bool, err error) {
	l := n.cfg.Listener
	if err := l.Connect(ctx); err != nil {
		return false, err //nolint:wrapcheck // adapter errors carry context
	}
	defer func() {
		cctx, cancel := context.WithTimeout(context.WithoutCancel(ctx), closeTimeout)
		defer cancel()
		_ = l.Close(cctx)
	}()
	for _, ch := range n.channels() {
		if err := l.Listen(ctx, ch); err != nil {
			return false, err //nolint:wrapcheck // adapter errors carry context
		}
	}
	n.connected.Store(true)
	if n.cfg.OnConnect != nil {
		n.cfg.OnConnect()
	}
	for {
		waitCtx, cancel := context.WithTimeout(ctx, n.cfg.IdleTimeout)
		msg, err := l.WaitForNotification(waitCtx)
		cancel()
		switch {
		case err == nil:
			n.dispatch(msg)
		case ctx.Err() == nil && errors.Is(err, context.DeadlineExceeded):
			// Quiet for IdleTimeout: make sure the peer is still there, since a
			// connection dropped without a FIN or RST never reports an error.
			if pingErr := n.ping(ctx); pingErr != nil {
				return true, pingErr
			}
		default:
			return true, err //nolint:wrapcheck // adapter errors carry context
		}
	}
}

func (n *Notifier) ping(ctx context.Context) error {
	pingCtx, cancel := context.WithTimeout(ctx, n.cfg.IdleTimeout)
	defer cancel()
	return n.cfg.Listener.Ping(pingCtx) //nolint:wrapcheck // adapter errors carry context
}

func (n *Notifier) dispatch(msg job.Notification) {
	n.mu.RLock()
	fns := n.subs[msg.Channel]
	n.mu.RUnlock()
	for _, fn := range fns {
		fn(msg.Payload)
	}
}
