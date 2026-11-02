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

// BatchHandler runs many jobs of one kind at once (sending many pushes concurrently, deleting many files). It returns
// one error per job, in the order given (nil: done); each job is settled on its own, so one bad job never fails the
// others. A shorter slice counts the missing ones as failed.
type BatchHandler func(ctx context.Context, jobs []Job) []error

// WorkOptions configure Work. The zero value is usable.
type WorkOptions struct {
	// Concurrency is how many jobs of each kind run at once in this process (default 1). For a batch handler, how
	// many batches.
	Concurrency int
	// BatchSize is how many jobs one fetch may take (default 1; up to the idle handlers, or the batch for a batch
	// handler). Drivers that are BatchReceivers fetch them in one call.
	BatchSize int
	// BatchHandlers handle these kinds in batches of up to BatchSize (a kind must not also be in handlers).
	BatchHandlers map[string]BatchHandler
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
	if o.BatchSize <= 0 {
		o.BatchSize = 1
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

// worker is one Work call's state.
type worker struct {
	q    *Queue
	o    *WorkOptions
	ctx  context.Context // ends: take no more
	hard context.Context // ends StopGrace later: running jobs stop

	keysMu  sync.Mutex
	keys    map[string]*keyLock
	ordered bool
}

type keyLock struct {
	mu   sync.Mutex
	refs int
}

// Work runs the handlers, one per kind, until ctx ends (pass a context cancelled on SIGTERM, e.g. from
// signal.NotifyContext). Then it takes no new job, gives running ones StopGrace to finish, cancels the rest (they are
// released and redelivered) and returns. If the Queue has a Scheduler, its loop runs here too.
func (q *Queue) Work(ctx context.Context, handlers map[string]Handler, o WorkOptions) error {
	o.defaults()
	if len(handlers)+len(o.BatchHandlers) == 0 {
		return errors.New("jobqueue: no handlers")
	}
	kinds := make([]string, 0, len(handlers)+len(o.BatchHandlers))
	for k := range handlers {
		kinds = append(kinds, k)
	}
	for k := range o.BatchHandlers {
		if _, dup := handlers[k]; dup {
			return fmt.Errorf("jobqueue: kind %q has a handler and a batch handler", k)
		}
		kinds = append(kinds, k)
	}
	for _, k := range kinds {
		if !ValidKind(k) {
			return fmt.Errorf("%w: %q", ErrInvalidKind, k)
		}
	}
	sort.Strings(kinds)

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

	w := &worker{q: q, o: &o, ctx: ctx, hard: hard, keys: map[string]*keyLock{}}
	if or, ok := q.d.(Orderer); ok && or.OrdersByKey() {
		w.ordered = true
	}
	var wg sync.WaitGroup
	if q.sched != nil {
		wg.Go(func() { q.sched.run(ctx, q) })
	}
	for _, kind := range kinds {
		if bh, ok := o.BatchHandlers[kind]; ok {
			for range o.Concurrency {
				wg.Go(func() { w.batchLoop(kind, bh) })
			}
			continue
		}
		wg.Go(func() { w.loop(kind, handlers[kind]) })
	}
	wg.Wait()
	return nil
}

// fetch takes up to max deliveries of kind (one call for a BatchReceiver).
func (w *worker) fetch(kind string, max int) ([]Delivery, error) {
	if br, ok := w.q.d.(BatchReceiver); ok && max > 1 {
		return br.ReceiveBatch(w.ctx, kind, max)
	}
	d, err := w.q.d.Receive(w.ctx, kind)
	if err != nil || d == nil {
		return nil, err
	}
	return []Delivery{d}, nil
}

func (w *worker) receiveFailed(kind string, err error) {
	if w.ctx.Err() != nil {
		return
	}
	w.o.Logger.Warn("jobqueue: receive", "kind", kind, "error", err.Error())
	sleep(w.ctx, time.Second)
}

// loop fetches jobs of one kind while handlers are free, and runs each on its own.
func (w *worker) loop(kind string, h Handler) {
	free := make(chan struct{}, w.o.Concurrency)
	for range w.o.Concurrency {
		free <- struct{}{}
	}
	var running sync.WaitGroup
	defer running.Wait()
	for w.ctx.Err() == nil {
		select {
		case <-free:
		case <-w.ctx.Done():
			return
		}
		n := 1
	more:
		for n < w.o.BatchSize {
			select {
			case <-free:
				n++
			default:
				break more
			}
		}
		ds, err := w.fetch(kind, n)
		if err != nil {
			w.receiveFailed(kind, err)
		}
		for i := len(ds); i < n; i++ {
			free <- struct{}{}
		}
		// Jobs with the same key in one fetch run one after the other, in the order fetched; others at once.
		for _, group := range byKey(ds) {
			running.Go(func() {
				for _, d := range group {
					if w.ctx.Err() != nil {
						_ = d.Retry(w.hard, 0) // taken while stopping: give it back at once
					} else {
						w.handle(d, h)
					}
					free <- struct{}{}
				}
			})
		}
	}
}

// byKey splits deliveries into groups that must run in order: one per key, and one per unkeyed delivery.
func byKey(ds []Delivery) [][]Delivery {
	var out [][]Delivery
	at := map[string]int{}
	for _, d := range ds {
		m := d.Message()
		if m.Key == "" {
			out = append(out, []Delivery{d})
			continue
		}
		k := m.Kind + "\x00" + m.Key
		if i, ok := at[k]; ok {
			out[i] = append(out[i], d)
			continue
		}
		at[k] = len(out)
		out = append(out, []Delivery{d})
	}
	return out
}

func (w *worker) lockKey(key string) func() {
	w.keysMu.Lock()
	k := w.keys[key]
	if k == nil {
		k = &keyLock{}
		w.keys[key] = k
	}
	k.refs++
	w.keysMu.Unlock()
	k.mu.Lock()
	return func() {
		k.mu.Unlock()
		w.keysMu.Lock()
		k.refs--
		if k.refs == 0 {
			delete(w.keys, key)
		}
		w.keysMu.Unlock()
	}
}

// extend keeps d hidden from other workers until the returned stop is called.
func (w *worker) extend(ds ...Delivery) (stop func()) {
	done := make(chan struct{})
	go func() {
		t := time.NewTicker(w.o.ExtendEvery)
		defer t.Stop()
		for {
			select {
			case <-done:
				return
			case <-t.C:
				for _, d := range ds {
					_ = d.Extend(w.hard)
				}
			}
		}
	}()
	return func() { close(done) }
}

// handle runs one delivery and settles it. A keyed job is retried in place (its followers wait) rather than
// released, so they never overtake it.
func (w *worker) handle(d Delivery, h Handler) {
	job := Job{Message: d.Message(), Attempt: d.Attempt()}
	if job.Attempt < 1 {
		job.Attempt = 1
	}
	if job.Key != "" && !w.ordered {
		defer w.lockKey(job.Kind + "\x00" + job.Key)()
	}
	start := time.Now()
	stop := w.extend(d)
	err := safeRun(w.hard, h, job)
	for err != nil && job.Key != "" && !IsPermanent(err) && job.Attempt < w.o.MaxAttempts && w.hard.Err() == nil {
		w.report(job, "retry", err, start)
		sleep(w.hard, w.o.Backoff(job.Attempt))
		if w.hard.Err() != nil {
			break
		}
		job.Attempt++
		err = safeRun(w.hard, h, job)
	}
	stop()
	w.settle(d, job, err, start)
}

func (w *worker) report(job Job, outcome string, err error, start time.Time) {
	if outcome == "retry" && err != nil {
		w.o.Logger.Info("jobqueue: retry", "kind", job.Kind, "id", job.ID, "attempt", job.Attempt, "error", err.Error())
	}
	if w.o.OnDone != nil {
		w.o.OnDone(job, outcome, err, time.Since(start))
	}
}

// settle acknowledges, releases, retries or dead-letters a handled delivery.
func (w *worker) settle(d Delivery, job Job, err error, start time.Time) {
	ctx, cancel := context.WithTimeout(context.WithoutCancel(w.ctx), 10*time.Second)
	defer cancel()
	switch {
	case err == nil:
		if aerr := d.Ack(ctx); aerr != nil {
			w.o.Logger.Warn("jobqueue: ack", "kind", job.Kind, "id", job.ID, "error", aerr.Error())
		}
		w.report(job, "done", nil, start)
	case w.hard.Err() != nil:
		_ = d.Retry(ctx, 0) // stopped mid-way: another worker takes it now
		w.report(job, "released", err, start)
	case IsPermanent(err) || job.Attempt >= w.o.MaxAttempts:
		w.o.Logger.Warn("jobqueue: dead letter", "kind", job.Kind, "id", job.ID, "attempt", job.Attempt, "error", err.Error())
		if derr := w.q.d.DeadLetter(ctx, job.Message, job.Attempt, err); derr != nil {
			w.o.Logger.Error("jobqueue: dead letter failed", "kind", job.Kind, "id", job.ID, "error", derr.Error())
			_ = d.Retry(ctx, w.o.Backoff(job.Attempt))
			w.report(job, "retry", err, start)
			return
		}
		_ = d.Ack(ctx)
		w.report(job, "dead", err, start)
	default:
		_ = d.Retry(ctx, w.o.Backoff(job.Attempt))
		w.report(job, "retry", err, start)
	}
}

// batchLoop fetches up to BatchSize jobs of one kind and hands them to the batch handler at once.
func (w *worker) batchLoop(kind string, bh BatchHandler) {
	for w.ctx.Err() == nil {
		ds, err := w.fetch(kind, w.o.BatchSize)
		if err != nil {
			w.receiveFailed(kind, err)
			continue
		}
		if len(ds) == 0 {
			continue
		}
		if w.ctx.Err() != nil {
			for _, d := range ds {
				_ = d.Retry(w.hard, 0)
			}
			return
		}
		jobs := make([]Job, len(ds))
		for i, d := range ds {
			jobs[i] = Job{Message: d.Message(), Attempt: max(d.Attempt(), 1)}
		}
		start := time.Now()
		stop := w.extend(ds...)
		errs := safeBatch(w.hard, bh, jobs)
		stop()
		var wg sync.WaitGroup
		for i, d := range ds {
			var e error
			if i < len(errs) {
				e = errs[i]
			} else {
				e = errors.New("jobqueue: the batch handler gave no result for this job")
			}
			wg.Go(func() { w.settle(d, jobs[i], e, start) })
		}
		wg.Wait()
	}
}

func safeBatch(ctx context.Context, bh BatchHandler, jobs []Job) (errs []error) {
	defer func() {
		if r := recover(); r != nil {
			errs = make([]error, len(jobs))
			for i := range errs {
				errs[i] = fmt.Errorf("jobqueue: batch handler panicked: %v", r)
			}
		}
	}()
	return bh(ctx, jobs)
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
