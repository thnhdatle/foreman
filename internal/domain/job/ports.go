// SPDX-License-Identifier: Apache-2.0

package job

import (
	"context"
	"time"
)

type KeyQuota struct {
	Key   string
	Quota int
}

type FetchParams struct {
	Queue       string
	ClientID    string
	Now         time.Time
	Limit       int
	Keyed       bool
	Quotas      []KeyQuota
	PerKeyLimit int
}

type KeyStat struct {
	Key         string
	Priority    int
	ScheduledAt time.Time
	ID          int64
	Running     int
}

type Fetcher interface {
	Fetch(ctx context.Context, p FetchParams) ([]*Row, error)
	KeyStats(ctx context.Context, queue string, limit int, withRunning bool) ([]KeyStat, error)
	QueueHeartbeat(ctx context.Context, queue string, now time.Time) (paused bool, err error)
}

type CompletionWriter interface {
	Complete(ctx context.Context, clientID string, cs []Completion) (applied []int64, err error)
}

type HeartbeatWriter interface {
	Heartbeat(ctx context.Context, clientID string, now time.Time, refs []Ref) (applied int, err error)
}

type CancelPoller interface {
	CancelRequested(ctx context.Context, ids []int64) ([]int64, error)
}

type LeaseStore interface {
	AcquireLease(ctx context.Context, name, id string, now time.Time, ttl time.Duration) (bool, error)
	RenewLease(ctx context.Context, name, id string, now time.Time, ttl time.Duration) (bool, error)
	ReleaseLease(ctx context.Context, name, id string) error
}

type MaintenanceStore interface {
	PromoteDue(ctx context.Context, now time.Time, limit int) (int, error)
	StuckJobs(ctx context.Context, cutoff time.Time, limit int) ([]*Row, error)
	Rescue(ctx context.Context, cutoff time.Time, cs []Completion) (int, error)
	DeleteFinalized(ctx context.Context, state State, cutoff time.Time, limit int) (int, error)
	Reindex(ctx context.Context) (bool, error)
	QueueDepths(ctx context.Context) ([]QueueDepth, error)
	LastTicks(ctx context.Context) (map[string]time.Time, error)
	EnqueueTick(ctx context.Context, name string, tick time.Time, p InsertParams) (inserted bool, err error)
	MarkPeriodicStart(ctx context.Context, name string, now time.Time) error
	PruneTicks(ctx context.Context, cutoff time.Time) (int, error)
}

type QueueDepth struct {
	Queue string
	State State
	Count int64
}

type Notification struct {
	Channel string
	Payload string
}

type Listener interface {
	Connect(ctx context.Context) error
	Listen(ctx context.Context, channel string) error
	WaitForNotification(ctx context.Context) (Notification, error)
	Ping(ctx context.Context) error
	Close(ctx context.Context) error
}
