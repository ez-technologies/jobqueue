package jobqueue

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"sort"
	"sync"
	"time"
)

// WorkOptions configure Work. The zero value is usable.
type WorkOptions struct {
	// Concurrency is how many jobs of each kind run at once in this process (default 1).
	Concurrency int
	// StopGrace is how long running jobs may finish once ctx ends (default 25s, under the ~30 s notice spot
	// machines get). Jobs still running then see their ctx cancelled and are released for another worker.
	StopGrace time.Duration
	// MaxAttempts is how many times a job is tried before it goes to the dead letters (default 5).
	MaxAttempts int
	// Backoff is the wait before attempt n+1 after attempt n failed (default DefaultBackoff).
	Backoff func(attempt int) time.Duration
	// ExtendEvery is how often a running job tells the broker it is still running (default 20s).
	ExtendEvery time.Duration
	// Logger receives failures (default slog.Default()).
	Logger *slog.Logger
	// OnDone, if set, is told about every handled job: its outcome is "done", "retry", "dead" or "released".
	OnDone func(job Job, outcome string, err error, took time.Duration)
}

// DefaultBackoff waits 5 s, 30 s, 2 min, 10 min, then 30 min between attempts.
func DefaultBackoff(attempt int) time.Duration {
	steps := []time.Duration{5 * time.Second, 30 * time.Second, 2 * time.Minute, 10 * time.Minute, 30 * time.Minute}
	if attempt < 1 {
		attempt = 1
	}
	if attempt > len(steps) {
		attempt = len(steps)
	}
	return steps[attempt-1]
}

func (o *WorkOptions) defaults() {
	if o.Concurrency <= 0 {
		o.Concurrency = 1
	}
	if o.StopGrace <= 0 {
		o.StopGrace = 25 * time.Second
	}
	if o.MaxAttempts <= 0 {
		o.MaxAttempts = 5
	}
	if o.Backoff == nil {
		o.Backoff = DefaultBackoff
	}
	if o.ExtendEvery <= 0 {
		o.ExtendEvery = 20 * time.Second
	}
	if o.Logger == nil {
		o.Logger = slog.Default()
	}
}

// Work runs the handlers, one per kind, until ctx ends (pass a context cancelled on SIGTERM, e.g. from
// signal.NotifyContext). Then it takes no new job, gives running ones StopGrace to finish, cancels the rest (they are
// released and redelivered) and returns. If the Queue has a Scheduler, its loop runs here too.
func (q *Queue) Work(ctx context.Context, handlers map[string]Handler, o WorkOptions) error {
	o.defaults()
	if len(handlers) == 0 {
		return errors.New("jobqueue: no handlers")
	}
	kinds := make([]string, 0, len(handlers))
	for k := range handlers {
		if !ValidKind(k) {
			return fmt.Errorf("%w: %q", ErrInvalidKind, k)
		}
		kinds = append(kinds, k)
	}
	sort.Strings(kinds)

	// hard ends StopGrace after ctx: running handlers see it.
	hard, cancelHard := context.WithCancel(context.WithoutCancel(ctx))
	defer cancelHard()
	stopTimer := context.AfterFunc(ctx, func() {
		t := time.NewTimer(o.StopGrace)
		defer t.Stop()
		select {
		case <-t.C:
			cancelHard()
		case <-hard.Done():
		}
	})
	defer stopTimer()

	var wg sync.WaitGroup
	if q.sched != nil {
		wg.Go(func() { q.sched.run(ctx, q) })
	}
	for _, kind := range kinds {
		for range o.Concurrency {
			wg.Go(func() { q.loop(ctx, hard, kind, handlers[kind], &o) })
		}
	}
	wg.Wait()
	return nil
}

// loop takes jobs of one kind until ctx ends.
func (q *Queue) loop(ctx, hard context.Context, kind string, h Handler, o *WorkOptions) {
	for ctx.Err() == nil {
		d, err := q.d.Receive(ctx, kind)
		if err != nil {
			if ctx.Err() != nil {
				return
			}
			o.Logger.Warn("jobqueue: receive", "kind", kind, "error", err.Error())
			sleep(ctx, time.Second)
			continue
		}
		if d == nil {
			continue
		}
		if ctx.Err() != nil && d != nil {
			// Taken while stopping: give it back at once.
			_ = d.Retry(hard, 0)
			return
		}
		q.handle(ctx, hard, d, h, o)
	}
}

// handle runs one delivery and settles it.
func (q *Queue) handle(ctx, hard context.Context, d Delivery, h Handler, o *WorkOptions) {
	job := Job{Message: d.Message(), Attempt: d.Attempt()}
	if job.Attempt < 1 {
		job.Attempt = 1
	}
	start := time.Now()
	done := make(chan struct{})
	go func() { // keep it ours while it runs
		t := time.NewTicker(o.ExtendEvery)
		defer t.Stop()
		for {
			select {
			case <-done:
				return
			case <-t.C:
				_ = d.Extend(hard)
			}
		}
	}()
	err := safeRun(hard, h, job)
	close(done)
	settle := context.WithoutCancel(ctx)
	settle, cancel := context.WithTimeout(settle, 10*time.Second)
	defer cancel()
	report := func(outcome string, err error) {
		if o.OnDone != nil {
			o.OnDone(job, outcome, err, time.Since(start))
		}
	}
	switch {
	case err == nil:
		if aerr := d.Ack(settle); aerr != nil {
			o.Logger.Warn("jobqueue: ack", "kind", job.Kind, "id", job.ID, "error", aerr.Error())
		}
		report("done", nil)
	case hard.Err() != nil:
		// Stopped mid-way: release it for another worker now.
		_ = d.Retry(settle, 0)
		report("released", err)
	case IsPermanent(err) || job.Attempt >= o.MaxAttempts:
		o.Logger.Warn("jobqueue: dead letter", "kind", job.Kind, "id", job.ID, "attempt", job.Attempt, "error", err.Error())
		if derr := q.d.DeadLetter(settle, job.Message, job.Attempt, err); derr != nil {
			o.Logger.Error("jobqueue: dead letter failed", "kind", job.Kind, "id", job.ID, "error", derr.Error())
			_ = d.Retry(settle, o.Backoff(job.Attempt))
			report("retry", err)
			return
		}
		_ = d.Ack(settle)
		report("dead", err)
	default:
		o.Logger.Info("jobqueue: retry", "kind", job.Kind, "id", job.ID, "attempt", job.Attempt, "error", err.Error())
		_ = d.Retry(settle, o.Backoff(job.Attempt))
		report("retry", err)
	}
}

// safeRun turns a panic in a handler into an error.
func safeRun(ctx context.Context, h Handler, job Job) (err error) {
	defer func() {
		if r := recover(); r != nil {
			err = fmt.Errorf("jobqueue: handler panicked: %v", r)
		}
	}()
	return h(ctx, job)
}

func sleep(ctx context.Context, d time.Duration) {
	t := time.NewTimer(d)
	defer t.Stop()
	select {
	case <-ctx.Done():
	case <-t.C:
	}
}
