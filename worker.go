package jobqueue

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"math/rand/v2"
	"sort"
	"sync"
	"time"
)

// BatchHandler runs many jobs of one kind at once (sending many pushes concurrently, deleting many files). It returns
// one error per job, in the order given (nil: done); each job is settled on its own, so one bad job never fails the
// others. A shorter slice counts the missing ones as failed.
type BatchHandler func(ctx context.Context, jobs []Job) []error

// Mode is how a queue treats keys.
type Mode string

const (
	// KeyedOrdered (the default): jobs with a Key run one at a time per key, in publish order; jobs without a key are
	// unordered.
	KeyedOrdered Mode = ""
	// Unordered ignores keys: every job is independent (the fastest).
	Unordered Mode = "unordered"
)

// RetryRoute is how a failed job comes back.
type RetryRoute string

const (
	// Republish (the default): a copy with RetryInfo is published again with the back-off as its delay (the
	// driver's delay: NATS schedules, SQS DelaySeconds, Kafka retry-topic tiers, RabbitMQ wait queues, the Redis
	// delayed set; else the Scheduler), and the original is acknowledged at once. Nothing waits behind it.
	Republish RetryRoute = ""
	// Nack lets the broker redeliver the same message after the back-off (NATS NakWithDelay, SQS visibility, Pub/Sub
	// ack deadline); no copy. On Kafka this is the same as Republish.
	Nack RetryRoute = "nack"
)

// KeyPolicy is what a keyed job that keeps failing does to its key (keyed-ordered mode only).
type KeyPolicy string

const (
	// Park (the default): after KeyAttempts in place, the job goes to the dead letters and its key is parked: the
	// key's following jobs go there too (in order) until the key is unparked. An alert is logged and OnParked called.
	Park KeyPolicy = ""
	// RetryInPlace retries in place up to MaxAttempts, then dead-letters the job and goes on with the key's next job
	// (that one job is out of order).
	RetryInPlace KeyPolicy = "retry"
)

// ErrTimeout is the failure of a job that ran past its timeout.
var ErrTimeout = errors.New("jobqueue: job timed out")

// ErrParked is the dead-letter reason of a job whose key is parked.
var ErrParked = errors.New("jobqueue: key parked")

// Lane is a group of work with its own queue, workers and limits, so slow work never shares a batch or a worker
// slot with fast work. A lane holds the jobs of its Kinds, and the jobs of any kind published with InLane(name).
type Lane struct {
	Kinds       []string
	Concurrency int
	BatchSize   int
	Timeout     time.Duration
}

// WorkOptions is a queue's configuration in one place. The zero value is usable: unordered unless keyed, one job
// at a time per kind, 5-minute timeout, 5 attempts with jittered back-off, failures re-published, keys parked after 3
// attempts.
type WorkOptions struct {
	// Mode: KeyedOrdered (default) or Unordered.
	Mode Mode
	// Concurrency is how many jobs of each kind (route) run at once in this process (default 1); the most in flight.
	Concurrency int
	// BatchSize is how many jobs one fetch may take (default 1; up to the idle handlers, or the batch for a batch
	// handler). Drivers that are BatchReceivers fetch them in one call.
	BatchSize int
	// BatchHandlers handle these kinds in batches of up to BatchSize (a kind must not also be in handlers).
	BatchHandlers map[string]BatchHandler
	// Timeout is how long one job may run (default 5 minutes); past it the job counts as failed and is re-published.
	// Handlers must stop when their ctx ends: one that does not keeps running in the background.
	Timeout time.Duration
	// MaxAttempts is how many times a job is tried before it goes to the dead letters (default 5).
	MaxAttempts int
	// Backoff is the wait before attempt n+1 after attempt n failed (default DefaultBackoff), spread by Jitter
	// (default 0.2: ±20%; negative: none).
	Backoff func(attempt int) time.Duration
	Jitter  float64
	// RetryRoute: Republish (default) or Nack.
	RetryRoute RetryRoute
	// KeyPolicy (Park, the default, or RetryInPlace) and KeyAttempts (default 3): a keyed job that keeps failing.
	KeyPolicy   KeyPolicy
	KeyAttempts int
	// Parked keeps parked keys (default: in this process only; pgq.ParkStore shares them).
	Parked ParkStore
	// OnParked is the alert when a key is parked (also logged as an error).
	OnParked func(job Job, err error)
	// Lanes by name; OnlyLanes, when set, are the lanes this process runs ("default" is the lane of everything
	// else).
	Lanes     map[string]Lane
	OnlyLanes []string
	// StopGrace is how long running jobs may finish once ctx ends (default 25s, under the ~30 s notice spot
	// machines get). Jobs still running then see their ctx cancelled and are released for another worker.
	StopGrace time.Duration
	// ExtendEvery is how often a running job tells the broker it is still running (default 20s).
	ExtendEvery time.Duration
	// Logger receives failures (default slog.Default()).
	Logger *slog.Logger
	// RetryHandlers run jobs that came back from the retry path (a re-published copy), instead of the Handler.
	RetryHandlers map[string]RetryHandler
	// ShouldSkip is asked before a retry (and before a first attempt with SkipFirstAttempt): a job it skips is
	// acknowledged as "skipped" with its reason (Supersede is the ready-made one). A skip is final.
	ShouldSkip       SkipFunc
	SkipFirstAttempt bool
	// OnDone, if set, is told about every handled job: "done", "retry", "dead", "parked", "skipped" or "released".
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
	if o.Timeout <= 0 {
		o.Timeout = 5 * time.Minute
	}
	if o.StopGrace <= 0 {
		o.StopGrace = 25 * time.Second
	}
	if o.MaxAttempts <= 0 {
		o.MaxAttempts = 5
	}
	if o.KeyAttempts <= 0 {
		o.KeyAttempts = 3
	}
	if o.Backoff == nil {
		o.Backoff = DefaultBackoff
	}
	if o.Jitter == 0 {
		o.Jitter = 0.2
	}
	if o.ExtendEvery <= 0 {
		o.ExtendEvery = 20 * time.Second
	}
	if o.Logger == nil {
		o.Logger = slog.Default()
	}
	if o.Parked == nil {
		o.Parked = newMemPark()
	}
}

// laneLimits are the limits one loop runs with.
type laneLimits struct {
	name        string
	concurrency int
	batch       int
	timeout     time.Duration
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
	for name := range o.Lanes {
		if !laneRE.MatchString(name) || name == "default" {
			return fmt.Errorf("jobqueue: invalid lane name %q", name)
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
	runs := func(lane string) bool {
		if len(o.OnlyLanes) == 0 {
			return true
		}
		for _, l := range o.OnlyLanes {
			if l == lane {
				return true
			}
		}
		return false
	}
	limits := func(name string, l Lane) laneLimits {
		ll := laneLimits{name: name, concurrency: o.Concurrency, batch: o.BatchSize, timeout: o.Timeout}
		if l.Concurrency > 0 {
			ll.concurrency = l.Concurrency
		}
		if l.BatchSize > 0 {
			ll.batch = l.BatchSize
		}
		if l.Timeout > 0 {
			ll.timeout = l.Timeout
		}
		return ll
	}
	laneOf := map[string]string{}
	for name, l := range o.Lanes {
		for _, k := range l.Kinds {
			laneOf[k] = name
		}
	}
	var wg sync.WaitGroup
	if q.sched != nil {
		wg.Go(func() { q.sched.run(ctx, q) })
	}
	start := func(route, kind string, ll laneLimits) {
		if bh, ok := o.BatchHandlers[kind]; ok {
			for range ll.concurrency {
				wg.Go(func() { w.batchLoop(route, bh, ll) })
			}
			return
		}
		wg.Go(func() { w.loop(route, handlers[kind], ll) })
	}
	for _, kind := range kinds {
		// The kind's own queue: in its lane when the lane lists it, else the default lane.
		own := "default"
		if name, ok := laneOf[kind]; ok {
			own = name
		}
		if runs(own) {
			start(kind, kind, limits(own, o.Lanes[own]))
		}
		// Jobs of this kind published into a lane (InLane).
		for name, l := range o.Lanes {
			if runs(name) {
				start(Message{Kind: kind, Lane: name}.Route(), kind, limits(name, l))
			}
		}
	}
	wg.Wait()
	return nil
}

// fetch takes up to max deliveries of a route (one call for a BatchReceiver).
func (w *worker) fetch(route string, max int) ([]Delivery, error) {
	if br, ok := w.q.d.(BatchReceiver); ok && max > 1 {
		return br.ReceiveBatch(w.ctx, route, max)
	}
	d, err := w.q.d.Receive(w.ctx, route)
	if err != nil || d == nil {
		return nil, err
	}
	return []Delivery{d}, nil
}

func (w *worker) receiveFailed(route string, err error) {
	if w.ctx.Err() != nil {
		return
	}
	w.o.Logger.Warn("jobqueue: receive", "route", route, "error", err.Error())
	sleep(w.ctx, time.Second)
}

// loop fetches jobs of one route while handlers are free, and runs each on its own.
func (w *worker) loop(route string, h Handler, ll laneLimits) {
	free := make(chan struct{}, ll.concurrency)
	for range ll.concurrency {
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
		for n < ll.batch {
			select {
			case <-free:
				n++
			default:
				break more
			}
		}
		ds, err := w.fetch(route, n)
		if err != nil {
			w.receiveFailed(route, err)
		}
		for i := len(ds); i < n; i++ {
			free <- struct{}{}
		}
		// Jobs with the same key in one fetch run one after the other, in the order fetched; others at once.
		for _, group := range w.byKey(ds) {
			running.Go(func() {
				for _, d := range group {
					if w.ctx.Err() != nil {
						_ = d.Retry(w.hard, 0) // taken while stopping: give it back at once
					} else {
						w.handle(d, h, ll)
					}
					free <- struct{}{}
				}
			})
		}
	}
}

// byKey splits deliveries into groups that must run in order: one per key (keyed-ordered mode), and one per other
// delivery.
func (w *worker) byKey(ds []Delivery) [][]Delivery {
	var out [][]Delivery
	at := map[string]int{}
	for _, d := range ds {
		m := d.Message()
		if m.Key == "" || w.o.Mode == Unordered {
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

// extend keeps deliveries hidden from other workers until the returned stop is called.
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

// jobOf is the job a delivery carries, its attempt counting earlier copies (RetryInfo) and redeliveries.
func jobOf(d Delivery) Job {
	m := d.Message()
	base := 1
	if m.Retry != nil && m.Retry.Attempt > 1 {
		base = m.Retry.Attempt
	}
	return Job{Message: m, Attempt: base + max(d.Attempt()-1, 0)}
}

func (w *worker) backoff(attempt int) time.Duration {
	b := w.o.Backoff(attempt)
	if w.o.Jitter > 0 && b > 0 {
		b = time.Duration(float64(b) * (1 + w.o.Jitter*(2*rand.Float64()-1)))
	}
	return b
}

// handle runs one delivery and settles it.
func (w *worker) handle(d Delivery, h Handler, ll laneLimits) {
	job := jobOf(d)
	start := time.Now()
	keyed := job.Key != "" && w.o.Mode != Unordered
	if keyed {
		if !w.ordered {
			defer w.lockKey(job.Kind + "\x00" + job.Key)()
		}
		if parked, err := w.o.Parked.Parked(w.hard, job.Kind, job.Key); err == nil && parked {
			// An earlier job of this key failed for good: this one follows it to the dead letters, in order.
			w.deadLetter(d, job, ErrParked, start, "parked")
			return
		}
	}
	if w.o.ShouldSkip != nil && (job.Retry != nil || w.o.SkipFirstAttempt) {
		if skip, reason := w.o.ShouldSkip(w.hard, job); skip {
			w.skip(d, job, reason, start)
			return
		}
	}
	if rh, ok := w.o.RetryHandlers[job.Kind]; ok && job.Retry != nil {
		info := *job.Retry
		h = func(ctx context.Context, j Job) error { return rh(ctx, j, info) }
	}
	stop := w.extend(d)
	err := w.run(h, job, ll.timeout)
	if keyed {
		// A keyed job is retried in place (only its key waits), never re-published behind its followers.
		limit := w.o.KeyAttempts
		if w.o.KeyPolicy == RetryInPlace {
			limit = w.o.MaxAttempts
		}
		for err != nil && !IsPermanent(err) && job.Attempt < limit && w.hard.Err() == nil {
			w.report(job, "retry", err, start)
			sleep(w.hard, w.backoff(job.Attempt))
			if w.hard.Err() != nil {
				break
			}
			job.Attempt++
			err = w.run(h, job, ll.timeout)
		}
	}
	stop()
	switch {
	case err == nil:
		w.ack(d, job, start)
	case w.hard.Err() != nil:
		w.release(d, job, err, start)
	case keyed && w.o.KeyPolicy != RetryInPlace:
		w.park(d, job, err, start)
	case keyed:
		w.deadLetter(d, job, err, start, "dead")
	default:
		w.fail(d, job, err, start)
	}
}

// run calls the handler with the job's timeout; past it the job has failed (even if the handler ignores its ctx).
func (w *worker) run(h Handler, job Job, timeout time.Duration) error {
	ctx, cancel := context.WithTimeout(w.hard, timeout)
	defer cancel()
	done := make(chan error, 1)
	go func() { done <- safeRun(ctx, h, job) }()
	select {
	case err := <-done:
		if err != nil && errors.Is(ctx.Err(), context.DeadlineExceeded) && w.hard.Err() == nil {
			return fmt.Errorf("%w after %s: %v", ErrTimeout, timeout, err)
		}
		return err
	case <-ctx.Done():
		if w.hard.Err() != nil {
			return w.hard.Err()
		}
		return fmt.Errorf("%w after %s", ErrTimeout, timeout)
	}
}

func (w *worker) report(job Job, outcome string, err error, start time.Time) {
	if outcome == "retry" && err != nil {
		w.o.Logger.Info("jobqueue: retry", "kind", job.Kind, "id", job.ID, "attempt", job.Attempt, "error", err.Error())
	}
	if w.o.OnDone != nil {
		w.o.OnDone(job, outcome, err, time.Since(start))
	}
}

func (w *worker) settleCtx() (context.Context, context.CancelFunc) {
	return context.WithTimeout(context.WithoutCancel(w.ctx), 10*time.Second)
}

func (w *worker) ack(d Delivery, job Job, start time.Time) {
	ctx, cancel := w.settleCtx()
	defer cancel()
	if err := d.Ack(ctx); err != nil {
		w.o.Logger.Warn("jobqueue: ack", "kind", job.Kind, "id", job.ID, "error", err.Error())
	}
	w.report(job, "done", nil, start)
}

// skip acknowledges a job that is not worth running any more.
func (w *worker) skip(d Delivery, job Job, reason string, start time.Time) {
	ctx, cancel := w.settleCtx()
	defer cancel()
	_ = d.Ack(ctx)
	w.o.Logger.Info("jobqueue: skipped", "kind", job.Kind, "id", job.ID, "attempt", job.Attempt, "reason", reason)
	w.report(job, "skipped", errors.New(reason), start)
}

// release gives a job back at once (the worker is stopping): not a failure.
func (w *worker) release(d Delivery, job Job, err error, start time.Time) {
	ctx, cancel := w.settleCtx()
	defer cancel()
	_ = d.Retry(ctx, 0)
	w.report(job, "released", err, start)
}

// fail handles a failed unkeyed job: dead letters after the last attempt, else re-published (or nacked) with its
// back-off, the original acknowledged at once.
func (w *worker) fail(d Delivery, job Job, err error, start time.Time) {
	if IsPermanent(err) || job.Attempt >= w.o.MaxAttempts {
		w.deadLetter(d, job, err, start, "dead")
		return
	}
	ctx, cancel := w.settleCtx()
	defer cancel()
	delay := w.backoff(job.Attempt)
	if w.o.RetryRoute == Nack {
		_ = d.Retry(ctx, delay)
		w.report(job, "retry", err, start)
		return
	}
	next := job.Message
	info := RetryInfo{Attempt: job.Attempt + 1, FirstFailedAt: time.Now().UTC(), LastError: clip(err.Error(), 1000),
		NextAt: time.Now().Add(delay).UTC()}
	if job.Retry != nil {
		info.Origin, info.FirstFailedAt = job.Retry.Origin, job.Retry.FirstFailedAt
	} else if o, ok := d.(Originer); ok {
		info.Origin = o.Origin()
	}
	next.Retry = &info
	if perr := w.q.publishCopy(ctx, next, delay); perr != nil {
		// Could not re-publish: let the broker bring it back instead.
		w.o.Logger.Warn("jobqueue: re-publish", "kind", job.Kind, "id", job.ID, "error", perr.Error())
		_ = d.Retry(ctx, delay)
	} else if aerr := d.Ack(ctx); aerr != nil {
		w.o.Logger.Warn("jobqueue: ack", "kind", job.Kind, "id", job.ID, "error", aerr.Error())
	}
	w.report(job, "retry", err, start)
}

// publishCopy sends a retry copy with a delay: the driver's, else the Scheduler's, else ErrNoScheduler.
func (q *Queue) publishCopy(ctx context.Context, m Message, delay time.Duration) error {
	if delay <= q.d.MaxDelay() {
		return q.d.Send(ctx, m, delay)
	}
	if q.sched == nil {
		return ErrNoScheduler
	}
	return q.sched.Store.Put(ctx, Entry{Message: m, Due: q.now().Add(delay)})
}

func (w *worker) deadLetter(d Delivery, job Job, err error, start time.Time, outcome string) {
	ctx, cancel := w.settleCtx()
	defer cancel()
	w.o.Logger.Warn("jobqueue: dead letter", "kind", job.Kind, "id", job.ID, "attempt", job.Attempt, "error", err.Error())
	if derr := w.q.d.DeadLetter(ctx, job.Message, job.Attempt, err); derr != nil {
		w.o.Logger.Error("jobqueue: dead letter failed", "kind", job.Kind, "id", job.ID, "error", derr.Error())
		_ = d.Retry(ctx, w.backoff(job.Attempt))
		w.report(job, "retry", err, start)
		return
	}
	_ = d.Ack(ctx)
	w.report(job, outcome, err, start)
}

// park dead-letters a keyed job that kept failing and parks its key: the key's next jobs follow it there.
func (w *worker) park(d Delivery, job Job, err error, start time.Time) {
	ctx, cancel := w.settleCtx()
	defer cancel()
	if perr := w.o.Parked.Park(ctx, job.Kind, job.Key, err.Error()); perr != nil {
		w.o.Logger.Error("jobqueue: park", "kind", job.Kind, "key", job.Key, "error", perr.Error())
	}
	w.o.Logger.Error("jobqueue: KEY PARKED: its jobs go to the dead letters until it is unparked",
		"kind", job.Kind, "key", job.Key, "id", job.ID, "attempt", job.Attempt, "error", err.Error())
	if w.o.OnParked != nil {
		w.o.OnParked(job, err)
	}
	w.deadLetter(d, job, err, start, "parked")
}

// batchLoop fetches up to the lane's batch of one route and hands them to the batch handler at once.
func (w *worker) batchLoop(route string, bh BatchHandler, ll laneLimits) {
	for w.ctx.Err() == nil {
		ds, err := w.fetch(route, ll.batch)
		if err != nil {
			w.receiveFailed(route, err)
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
		start := time.Now()
		var jobs []Job
		kept := ds[:0:0]
		for _, d := range ds {
			j := jobOf(d)
			if w.o.ShouldSkip != nil && (j.Retry != nil || w.o.SkipFirstAttempt) {
				if skip, reason := w.o.ShouldSkip(w.hard, j); skip {
					w.skip(d, j, reason, start)
					continue
				}
			}
			jobs, kept = append(jobs, j), append(kept, d)
		}
		ds = kept
		if len(ds) == 0 {
			continue
		}
		stop := w.extend(ds...)
		errs := w.runBatch(bh, jobs, ll.timeout)
		stop()
		var wg sync.WaitGroup
		for i, d := range ds {
			var e error
			if i < len(errs) {
				e = errs[i]
			} else {
				e = errors.New("jobqueue: the batch handler gave no result for this job")
			}
			wg.Go(func() {
				switch {
				case e == nil:
					w.ack(d, jobs[i], start)
				case w.hard.Err() != nil:
					w.release(d, jobs[i], e, start)
				default:
					w.fail(d, jobs[i], e, start)
				}
			})
		}
		wg.Wait()
	}
}

func (w *worker) runBatch(bh BatchHandler, jobs []Job, timeout time.Duration) []error {
	ctx, cancel := context.WithTimeout(w.hard, timeout)
	defer cancel()
	done := make(chan []error, 1)
	go func() { done <- safeBatch(ctx, bh, jobs) }()
	select {
	case errs := <-done:
		return errs
	case <-ctx.Done():
		errs := make([]error, len(jobs))
		for i := range errs {
			if w.hard.Err() != nil {
				errs[i] = w.hard.Err()
			} else {
				errs[i] = fmt.Errorf("%w after %s", ErrTimeout, timeout)
			}
		}
		return errs
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

func clip(s string, n int) string {
	if len(s) > n {
		return s[:n]
	}
	return s
}

func sleep(ctx context.Context, d time.Duration) {
	t := time.NewTimer(d)
	defer t.Stop()
	select {
	case <-ctx.Done():
	case <-t.C:
	}
}

// ParkStore remembers keys parked after a keyed job kept failing (KeyPolicy Park).
type ParkStore interface {
	Park(ctx context.Context, kind, key, reason string) error
	Parked(ctx context.Context, kind, key string) (bool, error)
	// Unpark lets the key's jobs run again (after the operator dealt with its dead letters).
	Unpark(ctx context.Context, kind, key string) error
}

// memPark is the default ParkStore: this process only.
type memPark struct {
	mu   sync.Mutex
	keys map[string]string
}

func newMemPark() *memPark { return &memPark{keys: map[string]string{}} }

// NewMemoryParkStore keeps parked keys in this process (the default).
func NewMemoryParkStore() ParkStore { return newMemPark() }

func (m *memPark) Park(_ context.Context, kind, key, reason string) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.keys[kind+"\x00"+key] = reason
	return nil
}

func (m *memPark) Parked(_ context.Context, kind, key string) (bool, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	_, ok := m.keys[kind+"\x00"+key]
	return ok, nil
}

func (m *memPark) Unpark(_ context.Context, kind, key string) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	delete(m.keys, kind+"\x00"+key)
	return nil
}
