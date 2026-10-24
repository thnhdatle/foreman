// SPDX-License-Identifier: Apache-2.0

package foreman

import (
	"context"
	"encoding/json"
	"sync"
	"time"

	"github.com/thnhdatle/foreman/internal/app/completer"
	"github.com/thnhdatle/foreman/internal/app/elector"
	"github.com/thnhdatle/foreman/internal/app/maintenance"
	"github.com/thnhdatle/foreman/internal/app/notifier"
	"github.com/thnhdatle/foreman/internal/app/producer"
	"github.com/thnhdatle/foreman/internal/domain/job"
)

const (
	leaderTTL   = elector.DefaultTTL
	cancelGrace = 5 * time.Second
)

type runState struct {
	c           *Client
	completer   *completer.Completer
	heartbeater *completer.Heartbeater
	producers   map[string]*producer.Producer
	elector     *elector.Elector
	notifier    *notifier.Notifier
	poller      *maintenance.CancelPoller

	fetchCancel, serviceCancel, completerCancel, electorCancel context.CancelFunc
	fetchWG, serviceWG, completerWG, electorWG, watchWG        sync.WaitGroup

	stopOnce  sync.Once
	stopWatch chan struct{}
	repoll    chan struct{}
}

func (c *Client) newRunState() (*runState, error) {
	r := &runState{
		c: c, producers: map[string]*producer.Producer{}, stopWatch: make(chan struct{}), repoll: make(chan struct{}, 1),
	}
	var err error
	r.completer, err = completer.New(completer.Config{
		ClientID: c.cfg.ID, Store: c.store, Clock: c.clock, Logger: c.logger,
		OnApplied: func(_ []job.Completion, stale int) { c.metrics.Stale.Add(float64(stale)) },
		OnDrained: r.wakeAll,
	})
	if err != nil {
		return nil, err //nolint:wrapcheck // configuration bug, already prefixed
	}
	r.heartbeater, err = completer.NewHeartbeater(completer.HeartbeatConfig{
		ClientID: c.cfg.ID, Store: c.store, Clock: c.clock, Logger: c.logger,
		Interval: c.cfg.HeartbeatInterval, Running: r.runningRefs,
	})
	if err != nil {
		return nil, err //nolint:wrapcheck // configuration bug, already prefixed
	}
	for name, q := range c.cfg.Queues {
		p, perr := producer.New(producer.Config{
			Queue: name, ClientID: c.cfg.ID, MaxWorkers: q.MaxWorkers, FetchInterval: q.FetchInterval,
			PerKeyLimit: q.PerKeyLimit, Fairness: q.Fairness, Store: c.store, Clock: c.clock, Logger: c.logger,
			Execute:  func(ctx context.Context, row *job.Row) { c.execute(ctx, r, row) },
			CanFetch: func() bool { return !r.completer.Full() },
			OnFetched: func(_ []*job.Row, d time.Duration) {
				c.metrics.FetchDuration.WithLabelValues(c.metrics.Queue(name)).Observe(d.Seconds())
			},
		})
		if perr != nil {
			return nil, perr //nolint:wrapcheck // configuration bug, already prefixed
		}
		r.producers[name] = p
	}
	if len(r.producers) == 0 {
		return r, nil
	}
	r.elector, err = elector.New(elector.Config{
		ClientID: c.cfg.ID, Store: c.store, Clock: c.clock, Logger: c.logger, TTL: leaderTTL,
		Services: r.leaderServices,
		OnChange: func(leader bool) {
			v := 0.0
			if leader {
				v = 1
			}
			c.metrics.Leader.Set(v)
		},
	})
	if err != nil {
		return nil, err //nolint:wrapcheck // configuration bug, already prefixed
	}
	r.poller = &maintenance.CancelPoller{
		Store: c.store, Clock: c.clock, Logger: c.logger, Interval: c.cfg.minFetchInterval(),
		Running: r.runningIDs, OnCancel: r.cancelJob,
	}
	if !c.cfg.PollOnly {
		r.notifier, err = notifier.New(notifier.Config{
			Listener: c.store.NewListener(), Clock: c.clock, Logger: c.logger, OnConnect: r.onConnect,
		})
		if err != nil {
			return nil, err //nolint:wrapcheck // configuration bug, already prefixed
		}
		names := c.store.Names()
		r.notifier.Subscribe(names.InsertChannel, r.onInsert)
		r.notifier.Subscribe(names.ControlChannel, r.onControl)
		r.notifier.Subscribe(names.LeaderChannel, func(string) { r.elector.Wake() })
	}
	return r, nil
}

func (r *runState) start(ctx context.Context) {
	base := context.WithoutCancel(ctx)
	fetchCtx, fetchCancel := context.WithCancel(base)
	serviceCtx, serviceCancel := context.WithCancel(base)
	completerCtx, completerCancel := context.WithCancel(base)
	electorCtx, electorCancel := context.WithCancel(base)
	r.fetchCancel, r.serviceCancel, r.completerCancel, r.electorCancel = fetchCancel, serviceCancel, completerCancel,
		electorCancel

	goRun(&r.completerWG, func() { r.logErr("completer", r.completer.Run(completerCtx)) })
	if len(r.producers) == 0 {
		return
	}
	goRun(&r.serviceWG, func() { _ = r.heartbeater.Run(serviceCtx) })
	if r.notifier != nil {
		goRun(&r.serviceWG, func() { _ = r.notifier.Run(serviceCtx) })
		goRun(&r.serviceWG, func() {
			for {
				select {
				case <-serviceCtx.Done():
					return
				case <-r.repoll:
					r.poller.Poll(serviceCtx)
				}
			}
		})
	} else {
		goRun(&r.serviceWG, func() { _ = r.poller.Run(serviceCtx) })
	}
	for _, p := range r.producers {
		goRun(&r.fetchWG, func() { _ = p.Run(fetchCtx) })
	}
	goRun(&r.electorWG, func() { _ = r.elector.Run(electorCtx) })
	r.watchWG.Add(1)
	go func() {
		defer r.watchWG.Done()
		select {
		case <-ctx.Done():
			go func() { _ = r.c.stopRun(context.WithoutCancel(ctx), r) }()
		case <-r.stopWatch:
		}
	}()
}

func goRun(wg *sync.WaitGroup, fn func()) {
	wg.Add(1)
	go func() {
		defer wg.Done()
		fn()
	}()
}

func (r *runState) logErr(component string, err error) {
	if err != nil {
		r.c.logger.Error("shutdown incomplete", "component", component, "error", err.Error())
	}
}

// shutdown stops fetching, waits for running jobs, cancels the rest and resigns leadership.
func (r *runState) shutdown(ctx context.Context, cancelJobs bool) (err error) {
	r.stopOnce.Do(func() {
		close(r.stopWatch)
		r.fetchCancel()
		r.fetchWG.Wait()
		if cancelJobs {
			r.cancelAll()
		}
		if werr := r.waitJobs(ctx); werr != nil {
			err = werr
			r.cancelAll()
			graceCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), cancelGrace)
			r.logErr("producer", r.waitJobs(graceCtx))
			cancel()
		}
		r.serviceCancel()
		r.serviceWG.Wait()
		r.completerCancel()
		r.completerWG.Wait()
		r.electorCancel()
		r.electorWG.Wait()
		r.watchWG.Wait()
		r.c.metrics.Leader.Set(0)
	})
	return err
}

func (r *runState) waitJobs(ctx context.Context) error {
	for _, p := range r.producers {
		if err := p.Wait(ctx); err != nil {
			return err //nolint:wrapcheck // ctx.Err() is returned to the caller as is
		}
	}
	return nil
}

func (r *runState) cancelAll() {
	for _, p := range r.producers {
		p.CancelAll()
	}
}

// crash simulates a process crash for tests.
func (r *runState) crash() {
	if r.elector != nil {
		r.elector.Crash()
	}
	r.fetchCancel()
	r.fetchWG.Wait()
	r.cancelAll()
	r.serviceCancel()
}

func (c *Client) stopRun(ctx context.Context, r *runState) error {
	c.mu.Lock()
	if c.run == r {
		c.run = nil
	}
	c.mu.Unlock()
	return r.shutdown(ctx, true)
}

func (r *runState) wakeAll() {
	for _, p := range r.producers {
		p.Wake()
	}
}

func (r *runState) onConnect() {
	r.wakeAll()
	select {
	case r.repoll <- struct{}{}:
	default:
	}
}

func (r *runState) onInsert(queue string) {
	if p, ok := r.producers[queue]; ok {
		p.Wake()
	}
}

func (r *runState) onControl(payload string) {
	var msg struct {
		Action string `json:"action"`
		JobID  int64  `json:"job_id"`
		Queue  string `json:"queue"`
	}
	if err := json.Unmarshal([]byte(payload), &msg); err != nil {
		r.c.logger.Warn("ignoring malformed control message", "error", err.Error())
		return
	}
	switch msg.Action {
	case "cancel":
		r.cancelJob(msg.JobID)
	case "pause", "resume":
		if p, ok := r.producers[msg.Queue]; ok {
			p.SetPaused(msg.Action == "pause")
		}
	}
}

func (r *runState) cancelJob(id int64) {
	for _, p := range r.producers {
		if p.Cancel(id) {
			return
		}
	}
}

func (r *runState) runningRefs() []job.Ref {
	var out []job.Ref
	for _, p := range r.producers {
		out = append(out, p.Running()...)
	}
	return out
}

func (r *runState) runningIDs() []int64 {
	refs := r.runningRefs()
	ids := make([]int64, len(refs))
	for i, ref := range refs {
		ids[i] = ref.ID
	}
	return ids
}

func (r *runState) leaderServices(ctx context.Context) {
	c := r.c
	base := maintenance.Base{Store: c.store, Clock: c.clock, Logger: c.logger, Observe: c.metrics.ObserveMaintenance}
	periodic := make([]maintenance.PeriodicJob, 0, len(c.cfg.PeriodicJobs))
	for _, pj := range c.cfg.PeriodicJobs {
		s, err := pj.resolvedSchedule()
		if err != nil {
			continue
		}
		periodic = append(periodic, maintenance.PeriodicJob{
			Name: pj.Name, Schedule: s, CatchUp: pj.CatchUp, RunOnStart: pj.RunOnStart, Build: c.periodicBuilder(pj),
		})
	}
	maintenance.RunAll(ctx,
		&maintenance.Scheduler{Base: base},
		&maintenance.Rescuer{
			Base: base, Interval: c.cfg.HeartbeatInterval, StuckAfter: c.cfg.RescueStuckJobsAfter,
			Retry: c.rescueRetry, OnRescued: func(n int) { c.metrics.Rescued.Add(float64(n)) },
		},
		&maintenance.Cleaner{Base: base, Interval: c.cfg.CleanerInterval, Retention: map[job.State]time.Duration{
			job.StateCompleted: c.cfg.CompletedJobRetention, job.StateCancelled: c.cfg.CancelledJobRetention,
			job.StateDiscarded: c.cfg.DiscardedJobRetention,
		}},
		&maintenance.Reindexer{Base: base, Interval: max(c.cfg.ReindexInterval, 0)},
		&maintenance.Periodic{
			Base: base, Jobs: periodic,
			OnSkipped: func(name string, n int) {
				c.metrics.PeriodicSkipped.WithLabelValues(c.metrics.Name(name)).Add(float64(n))
			},
		},
		&maintenance.DepthSampler{Base: base, OnDepths: c.recordDepths},
	)
}

func (c *Client) rescueRetry(row *job.Row, now time.Time) time.Time {
	if w, ok := c.cfg.Workers.byKind[row.Kind]; ok && w.retry != nil {
		return w.retry.NextRetry(*toJobRow(row), now)
	}
	return c.cfg.RetryPolicy.NextRetry(*toJobRow(row), now)
}

func (c *Client) recordDepths(depths []job.QueueDepth) {
	c.metrics.QueueDepth.Reset()
	for _, d := range depths {
		c.metrics.QueueDepth.WithLabelValues(c.metrics.Queue(d.Queue), string(d.State)).Set(float64(d.Count))
	}
}

func (c *Client) periodicBuilder(pj *PeriodicJob) func(tick time.Time) (job.InsertParams, error) {
	return func(tick time.Time) (job.InsertParams, error) {
		args, opts := pj.Args()
		if args == nil {
			return job.InsertParams{}, errNilPeriodicArgs
		}
		p, err := c.buildInsert(args, opts, c.clock.Now())
		if err != nil {
			return p, err
		}
		meta := map[string]any{}
		if len(p.Metadata) > 0 {
			if uerr := json.Unmarshal(p.Metadata, &meta); uerr != nil {
				return p, uerr //nolint:wrapcheck // validated JSON object
			}
		}
		meta["foreman_periodic"] = pj.Name
		meta["foreman_tick_at"] = tick.UTC().Format(time.RFC3339Nano)
		p.Metadata, err = json.Marshal(meta)
		return p, err //nolint:wrapcheck // marshalling a map of strings cannot fail
	}
}
