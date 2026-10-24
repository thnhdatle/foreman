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

// Listener holds a dedicated connection for LISTEN.
type Listener struct {
	pool *pgxpool.Pool
	conn *pgx.Conn
}

// NewListener returns a listener on the pool.
func (s *Store) NewListener() *Listener { return &Listener{pool: s.pool} }

func (l *Listener) Connect(ctx context.Context) error {
	c, err := l.pool.Acquire(ctx)
	if err != nil {
		return fmt.Errorf("pgstore: listener connect: %w", err)
	}
	l.conn = c.Hijack()
	return nil
}

func (l *Listener) Listen(ctx context.Context, channel string) error {
	if l.conn == nil {
		return errors.New("pgstore: listener not connected")
	}
	if _, err := l.conn.Exec(ctx, "LISTEN "+pgx.Identifier{channel}.Sanitize()); err != nil {
		return fmt.Errorf("pgstore: listen %s: %w", channel, err)
	}
	return nil
}

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

func (l *Listener) Ping(ctx context.Context) error {
	if l.conn == nil {
		return errors.New("pgstore: listener not connected")
	}
	if err := l.conn.Ping(ctx); err != nil {
		return fmt.Errorf("pgstore: listener ping: %w", err)
	}
	return nil
}

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
