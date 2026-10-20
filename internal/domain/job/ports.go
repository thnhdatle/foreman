// SPDX-License-Identifier: Apache-2.0

package job

import (
	"context"
	"time"
)

// KeyQuota is the maximum number of jobs to fetch for one concurrency key;
// Key "" is the bucket of jobs without a key.
type KeyQuota struct {
	Key   string
	Quota int
}

// FetchParams describes one fetch round of a producer.
type FetchParams struct {
	Queue    string
	ClientID string
	Now      time.Time
	// Limit is the number of jobs to take in plain (unkeyed) mode.
	Limit int
	// Keyed switches to per-key fetching with Quotas.
	Keyed  bool
	Quotas []KeyQuota
	// PerKeyLimit > 0 serialises keyed fetches per key and caps running jobs
	// per key at this value across the cluster.
	PerKeyLimit int
}

// KeyStat describes a concurrency key with available jobs: its head job in
// fetch order and how many of its jobs are running.
type KeyStat struct {
	Key         string
	Priority    int
	ScheduledAt time.Time
	ID          int64
	Running     int
}

// Fetcher is the store port of a producer.
type Fetcher interface {
	// Fetch locks available jobs with SKIP LOCKED, marks them running for
	// p.ClientID and returns them in fetch order.
	Fetch(ctx context.Context, p FetchParams) ([]*Row, error)
	// KeyStats lists up to limit keys that have available jobs in queue;
	// running counts are filled only when withRunning is true.
	KeyStats(ctx context.Context, queue string, limit int, withRunning bool) ([]KeyStat, error)
	// QueueHeartbeat records that a producer serves queue at now and reports
	// whether the queue is paused.
	QueueHeartbeat(ctx context.Context, queue string, now time.Time) (paused bool, err error)
}

// CompletionWriter applies batched state updates under the completion guard.
type CompletionWriter interface {
	// Complete applies cs for clientID and returns the ids that passed the guard.
	Complete(ctx context.Context, clientID string, cs []Completion) (applied []int64, err error)
}

// HeartbeatWriter refreshes heartbeat_at of running jobs under the completion guard.
type HeartbeatWriter interface {
	Heartbeat(ctx context.Context, clientID string, now time.Time, refs []Ref) (applied int, err error)
}

// CancelPoller reports which of the given running jobs have a pending cancel
// request (PollOnly mode, and after a notifier reconnect).
type CancelPoller interface {
	CancelRequested(ctx context.Context, ids []int64) ([]int64, error)
}

// LeaseStore is the leader election port (lease row, client clock).
type LeaseStore interface {
	// AcquireLease takes the lease if it is free, expired at now, or already held by id.
	AcquireLease(ctx context.Context, name, id string, now time.Time, ttl time.Duration) (bool, error)
	// RenewLease extends the lease if id still holds it.
	RenewLease(ctx context.Context, name, id string, now time.Time, ttl time.Duration) (bool, error)
	// ReleaseLease deletes the lease if id holds it and wakes other clients.
	ReleaseLease(ctx context.Context, name, id string) error
}

// MaintenanceStore is what leader-only maintenance services need.
type MaintenanceStore interface {
	// PromoteDue moves scheduled and retryable jobs whose time has come to available.
	PromoteDue(ctx context.Context, now time.Time, limit int) (int, error)
	// StuckJobs lists running jobs whose last heartbeat is before cutoff.
	StuckJobs(ctx context.Context, cutoff time.Time, limit int) ([]*Row, error)
	// Rescue applies rescue completions to jobs still running with a heartbeat before cutoff.
	Rescue(ctx context.Context, cutoff time.Time, cs []Completion) (int, error)
	// DeleteFinalized deletes up to limit jobs in state finalized before cutoff.
	DeleteFinalized(ctx context.Context, state State, cutoff time.Time, limit int) (int, error)
	// Reindex rebuilds the queue indexes concurrently unless a reindex is already running.
	Reindex(ctx context.Context) (bool, error)
	// QueueDepths counts jobs per queue and state.
	QueueDepths(ctx context.Context) ([]QueueDepth, error)
	// LastTicks returns the latest enqueued tick per periodic job name.
	LastTicks(ctx context.Context) (map[string]time.Time, error)
	// EnqueueTick records tick for name and inserts p in the same transaction;
	// inserted is false when the tick was already enqueued.
	EnqueueTick(ctx context.Context, name string, tick time.Time, p InsertParams) (inserted bool, err error)
	// MarkPeriodicStart records that name was first scheduled at now (a tick
	// row without a job), so a later leader catches up from it.
	MarkPeriodicStart(ctx context.Context, name string, now time.Time) error
	// PruneTicks deletes tick records older than cutoff except the latest per name.
	PruneTicks(ctx context.Context, cutoff time.Time) (int, error)
}

// QueueDepth is the number of jobs of a queue in one state.
type QueueDepth struct {
	Queue string
	State State
	Count int64
}

// Notification is one Postgres NOTIFY message.
type Notification struct {
	Channel string
	Payload string
}

// Listener is a dedicated LISTEN connection.
type Listener interface {
	Connect(ctx context.Context) error
	Listen(ctx context.Context, channel string) error
	WaitForNotification(ctx context.Context) (Notification, error)
	// Ping checks that the connection is still alive.
	Ping(ctx context.Context) error
	Close(ctx context.Context) error
}
