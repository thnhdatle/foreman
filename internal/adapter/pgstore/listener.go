// SPDX-License-Identifier: Apache-2.0

package pgstore

import (
	"context"
	"errors"
	"fmt"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/thnhdatle/foreman/internal/domain/job"
)

// Listener holds a dedicated connection for LISTEN; it implements job.Listener.
// The connection is taken out of the pool for its whole life and closed on
// Close, so a broken connection is never returned to the pool.
type Listener struct {
	pool *pgxpool.Pool
	conn *pgx.Conn // owned by the notifier goroutine between Connect and Close
}

// NewListener returns a listener on pool.
func (s *Store) NewListener() *Listener { return &Listener{pool: s.pool} }

// Connect acquires and detaches a connection from the pool.
func (l *Listener) Connect(ctx context.Context) error {
	c, err := l.pool.Acquire(ctx)
	if err != nil {
		return fmt.Errorf("pgstore: listener connect: %w", err)
	}
	l.conn = c.Hijack()
	return nil
}

// Listen subscribes to channel.
func (l *Listener) Listen(ctx context.Context, channel string) error {
	if l.conn == nil {
		return errors.New("pgstore: listener not connected")
	}
	if _, err := l.conn.Exec(ctx, "LISTEN "+pgx.Identifier{channel}.Sanitize()); err != nil {
		return fmt.Errorf("pgstore: listen %s: %w", channel, err)
	}
	return nil
}

// WaitForNotification blocks until a notification arrives or ctx is done.
func (l *Listener) WaitForNotification(ctx context.Context) (job.Notification, error) {
	if l.conn == nil {
		return job.Notification{}, errors.New("pgstore: listener not connected")
	}
	n, err := l.conn.WaitForNotification(ctx)
	if err != nil {
		return job.Notification{}, fmt.Errorf("pgstore: wait for notification: %w", err)
	}
	return job.Notification{Channel: n.Channel, Payload: n.Payload}, nil
}

// Ping runs an empty statement on the connection.
func (l *Listener) Ping(ctx context.Context) error {
	if l.conn == nil {
		return errors.New("pgstore: listener not connected")
	}
	if err := l.conn.Ping(ctx); err != nil {
		return fmt.Errorf("pgstore: listener ping: %w", err)
	}
	return nil
}

// Close closes the connection.
func (l *Listener) Close(ctx context.Context) error {
	if l.conn == nil {
		return nil
	}
	err := l.conn.Close(ctx)
	l.conn = nil
	if err != nil {
		return fmt.Errorf("pgstore: listener close: %w", err)
	}
	return nil
}
