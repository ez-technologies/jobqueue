// Package jobqueuetest is the conformance suite every jobqueue adapter passes. Call it from an adapter's tests:
//
//	func TestDriver(t *testing.T) { jobqueuetest.Driver(t, func(t *testing.T) jobqueue.Driver { return newDriver(t) }) }
//
// Kinds are unique per run, so the suite can share one broker (or emulator) with other tests.
package jobqueuetest

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/ez-technologies/jobqueue"
)

var seq atomic.Int64

// Kind is a kind unique to this run, for tests sharing one broker.
func Kind(name string) string {
	return fmt.Sprintf("t%d-%d.%s", time.Now().UnixNano()%1_000_000_000, seq.Add(1), name)
}

func receive(t *testing.T, d jobqueue.Driver, kind string, within time.Duration) jobqueue.Delivery {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), within)
	defer cancel()
	for ctx.Err() == nil {
		got, err := d.Receive(ctx, kind)
		if err != nil && ctx.Err() == nil {
			t.Fatalf("receive: %v", err)
		}
		if got != nil {
			return got
		}
	}
	return nil
}

// Driver checks a Driver: send, receive, ack, retry, delay, kinds kept apart, dead letters, and a Queue working over
// it (retries, dead letters, a graceful stop releasing a running job).
func Driver(t *testing.T, newDriver func(t *testing.T) jobqueue.Driver) {
	ctx := context.Background()

	t.Run("SendReceiveAck", func(t *testing.T) {
		d := newDriver(t)
		kind := Kind("basic")
		m := jobqueue.Message{ID: "job-1", Kind: kind, Payload: json.RawMessage(`{"n":1}`)}
		if err := d.Send(ctx, m, 0); err != nil {
			t.Fatalf("send: %v", err)
		}
		got := receive(t, d, kind, 15*time.Second)
		if got == nil {
			t.Fatal("nothing received")
		}
		gm := got.Message()
		if gm.ID != m.ID || gm.Kind != kind || string(gm.Payload) != `{"n":1}` {
			t.Fatalf("got %+v, want %+v", gm, m)
		}
		if got.Attempt() != 1 {
			t.Fatalf("attempt %d, want 1", got.Attempt())
		}
		if err := got.Extend(ctx); err != nil {
			t.Fatalf("extend: %v", err)
		}
		if err := got.Ack(ctx); err != nil {
			t.Fatalf("ack: %v", err)
		}
		if again := receive(t, d, kind, 3*time.Second); again != nil {
			t.Fatalf("acknowledged message came again: %+v", again.Message())
		}
	})

	t.Run("RetryRedeliversWithTheNextAttempt", func(t *testing.T) {
		d := newDriver(t)
		kind := Kind("retry")
		if err := d.Send(ctx, jobqueue.Message{ID: "job-r", Kind: kind}, 0); err != nil {
			t.Fatal(err)
		}
		first := receive(t, d, kind, 15*time.Second)
		if first == nil {
			t.Fatal("nothing received")
		}
		if err := first.Retry(ctx, time.Second); err != nil {
			t.Fatalf("retry: %v", err)
		}
		second := receive(t, d, kind, 30*time.Second)
		if second == nil {
			t.Fatal("not redelivered")
		}
		if second.Message().ID != "job-r" {
			t.Fatalf("got %q", second.Message().ID)
		}
		if second.Attempt() != 2 {
			t.Fatalf("attempt %d, want 2", second.Attempt())
		}
		_ = second.Ack(ctx)
	})

	t.Run("KindsAreKeptApart", func(t *testing.T) {
		d := newDriver(t)
		a, b := Kind("a"), Kind("b")
		if err := d.Send(ctx, jobqueue.Message{ID: "only-a", Kind: a}, 0); err != nil {
			t.Fatal(err)
		}
		if got := receive(t, d, b, 2*time.Second); got != nil {
			t.Fatalf("kind %s got %+v", b, got.Message())
		}
		got := receive(t, d, a, 15*time.Second)
		if got == nil {
			t.Fatal("nothing for kind a")
		}
		_ = got.Ack(ctx)
	})

	t.Run("Delay", func(t *testing.T) {
		d := newDriver(t)
		if d.MaxDelay() < 3*time.Second {
			t.Skip("the driver holds no delay of its own; the Scheduler does (see SchedulerOver)")
		}
		kind := Kind("delay")
		start := time.Now()
		if err := d.Send(ctx, jobqueue.Message{ID: "late", Kind: kind}, 3*time.Second); err != nil {
			t.Fatal(err)
		}
		if got := receive(t, d, kind, 1500*time.Millisecond); got != nil {
			t.Fatalf("delivered after %s, before its delay", time.Since(start))
		}
		got := receive(t, d, kind, 20*time.Second)
		if got == nil {
			t.Fatal("delayed message never came")
		}
		if waited := time.Since(start); waited < 2*time.Second {
			t.Fatalf("came after %s", waited)
		}
		_ = got.Ack(ctx)
	})

	t.Run("DeadLetter", func(t *testing.T) {
		d := newDriver(t)
		if err := d.DeadLetter(ctx, jobqueue.Message{ID: "bad", Kind: Kind("dead"), Payload: json.RawMessage(`{}`)}, 5,
			errors.New("it kept failing")); err != nil {
			t.Fatalf("dead letter: %v", err)
		}
	})

	t.Run("QueueRetriesThenSucceeds", func(t *testing.T) {
		d := newDriver(t)
		q := jobqueue.New(d)
		kind := Kind("work")
		var calls atomic.Int32
		done := make(chan jobqueue.Job, 4)
		h := func(ctx context.Context, j jobqueue.Job) error {
			if calls.Add(1) == 1 {
				return errors.New("first time fails")
			}
			done <- j
			return nil
		}
		if err := q.Publish(ctx, kind, "w-1", map[string]int{"x": 7}); err != nil {
			t.Fatal(err)
		}
		wctx, stop := context.WithCancel(ctx)
		finished := make(chan struct{})
		go func() {
			_ = q.Work(wctx, map[string]jobqueue.Handler{kind: h}, jobqueue.WorkOptions{Backoff: func(int) time.Duration { return time.Second }})
			close(finished)
		}()
		select {
		case j := <-done:
			var p struct{ X int }
			if err := j.Decode(&p); err != nil || p.X != 7 {
				t.Fatalf("payload %s: %v", j.Payload, err)
			}
			if j.Attempt != 2 {
				t.Fatalf("attempt %d, want 2", j.Attempt)
			}
		case <-time.After(40 * time.Second):
			t.Fatal("job never succeeded")
		}
		stop()
		<-finished
	})

	t.Run("QueueDeadLettersAfterTheLastAttemptOrAPermanentError", func(t *testing.T) {
		d := newDriver(t)
		q := jobqueue.New(d)
		kind := Kind("fail")
		var mu sync.Mutex
		outcomes := map[string][]string{}
		h := func(ctx context.Context, j jobqueue.Job) error {
			if j.ID == "perm" {
				return jobqueue.Permanent(errors.New("bad input"))
			}
			return errors.New("always fails")
		}
		for _, id := range []string{"perm", "flaky"} {
			if err := q.Publish(ctx, kind, id, nil); err != nil {
				t.Fatal(err)
			}
		}
		wctx, stop := context.WithCancel(ctx)
		defer stop()
		go func() {
			_ = q.Work(wctx, map[string]jobqueue.Handler{kind: h}, jobqueue.WorkOptions{
				MaxAttempts: 2, Backoff: func(int) time.Duration { return time.Second },
				OnDone: func(j jobqueue.Job, outcome string, _ error, _ time.Duration) {
					mu.Lock()
					outcomes[j.ID] = append(outcomes[j.ID], outcome)
					mu.Unlock()
				}})
		}()
		deadline := time.Now().Add(40 * time.Second)
		for time.Now().Before(deadline) {
			mu.Lock()
			p, f := fmt.Sprint(outcomes["perm"]), fmt.Sprint(outcomes["flaky"])
			mu.Unlock()
			if p == "[dead]" && f == "[retry dead]" {
				return
			}
			time.Sleep(100 * time.Millisecond)
		}
		mu.Lock()
		defer mu.Unlock()
		t.Fatalf("outcomes %v, want perm [dead], flaky [retry dead]", outcomes)
	})

	t.Run("QueueStopsGracefullyAndReleasesWhatRuns", func(t *testing.T) {
		d := newDriver(t)
		q := jobqueue.New(d)
		kind := Kind("stop")
		started := make(chan struct{}, 1)
		h := func(ctx context.Context, j jobqueue.Job) error {
			started <- struct{}{}
			<-ctx.Done() // a long job: runs until the worker must stop
			return ctx.Err()
		}
		if err := q.Publish(ctx, kind, "long", nil); err != nil {
			t.Fatal(err)
		}
		wctx, stop := context.WithCancel(ctx)
		var outcome atomic.Value
		finished := make(chan struct{})
		go func() {
			_ = q.Work(wctx, map[string]jobqueue.Handler{kind: h}, jobqueue.WorkOptions{StopGrace: 500 * time.Millisecond,
				OnDone: func(_ jobqueue.Job, o string, _ error, _ time.Duration) { outcome.Store(o) }})
			close(finished)
		}()
		select {
		case <-started:
		case <-time.After(20 * time.Second):
			t.Fatal("job never started")
		}
		stopAt := time.Now()
		stop()
		select {
		case <-finished:
		case <-time.After(15 * time.Second):
			t.Fatal("Work did not return after its stop grace")
		}
		if took := time.Since(stopAt); took < 400*time.Millisecond {
			t.Fatalf("returned after %s, before the grace", took)
		}
		if outcome.Load() != "released" {
			t.Fatalf("outcome %v, want released", outcome.Load())
		}
		again := receive(t, d, kind, 30*time.Second)
		if again == nil {
			t.Fatal("the released job was not delivered again")
		}
		if again.Message().ID != "long" {
			t.Fatalf("got %q", again.Message().ID)
		}
		_ = again.Ack(ctx)
	})
	t.Run("KeyedJobsRunOneAtATimeInPublishOrder", func(t *testing.T) { Ordering(t, newDriver) })
	t.Run("BatchesArePublishedFetchedAndSettledOneByOne", func(t *testing.T) { Batches(t, newDriver) })
	t.Run("Flow", func(t *testing.T) { Flow(t, newDriver) })
}

// Ordering publishes interleaved jobs for three keys and checks that each key's jobs ran one at a time, in publish
// order, with two workers (two drivers) for a driver that orders by key across processes, else one.
func Ordering(t *testing.T, newDriver func(t *testing.T) jobqueue.Driver) {
	ctx := context.Background()
	kind := Kind("ordered")
	d1 := newDriver(t)
	drivers := []jobqueue.Driver{d1}
	if o, ok := d1.(jobqueue.Orderer); ok && o.OrdersByKey() {
		drivers = append(drivers, newDriver(t))
	}
	q := jobqueue.New(d1)
	keys := []string{"k1", "k2", "k3"}
	const per = 6
	for i := 0; i < per; i++ {
		for _, k := range keys {
			if err := q.Publish(ctx, kind, fmt.Sprintf("%s-%02d", k, i), map[string]int{"n": i}, jobqueue.Key(k)); err != nil {
				t.Fatal(err)
			}
		}
	}
	var mu sync.Mutex
	seen := map[string][]int{}
	running := map[string]int{}
	var overlap atomic.Bool
	total := atomic.Int32{}
	h := func(ctx context.Context, j jobqueue.Job) error {
		var p struct{ N int }
		_ = j.Decode(&p)
		mu.Lock()
		running[j.Key]++
		if running[j.Key] > 1 {
			overlap.Store(true)
		}
		mu.Unlock()
		time.Sleep(20 * time.Millisecond)
		mu.Lock()
		running[j.Key]--
		seen[j.Key] = append(seen[j.Key], p.N)
		mu.Unlock()
		total.Add(1)
		return nil
	}
	wctx, stop := context.WithCancel(ctx)
	var wg sync.WaitGroup
	for _, d := range drivers {
		wg.Go(func() {
			_ = jobqueue.New(d).Work(wctx, map[string]jobqueue.Handler{kind: h}, jobqueue.WorkOptions{Concurrency: 4, BatchSize: 4})
		})
	}
	deadline := time.Now().Add(60 * time.Second)
	for total.Load() < int32(per*len(keys)) && time.Now().Before(deadline) {
		time.Sleep(50 * time.Millisecond)
	}
	stop()
	wg.Wait()
	if total.Load() != int32(per*len(keys)) {
		t.Fatalf("ran %d of %d", total.Load(), per*len(keys))
	}
	if overlap.Load() {
		t.Fatal("two jobs with the same key ran at once")
	}
	for _, k := range keys {
		for i, n := range seen[k] {
			if n != i {
				t.Fatalf("key %s ran in order %v", k, seen[k])
			}
		}
	}
}

// Batches publishes 25 jobs in one batch and handles them with a batch handler (batches of up to 10): every job is
// done (at least once), and the one that fails is retried on its own.
func Batches(t *testing.T, newDriver func(t *testing.T) jobqueue.Driver) {
	ctx := context.Background()
	d := newDriver(t)
	q := jobqueue.New(d)
	kind := Kind("batch")
	var pubs []jobqueue.Pub
	for i := 0; i < 25; i++ {
		pubs = append(pubs, jobqueue.Pub{Kind: kind, ID: fmt.Sprintf("b-%02d", i), Payload: map[string]int{"n": i}})
	}
	if err := q.PublishBatch(ctx, pubs); err != nil {
		t.Fatalf("publish batch: %v", err)
	}
	var mu sync.Mutex
	done := map[string]int{}
	failedOnce := false
	biggest := 0
	bh := func(ctx context.Context, jobs []jobqueue.Job) []error {
		mu.Lock()
		defer mu.Unlock()
		biggest = max(biggest, len(jobs))
		errs := make([]error, len(jobs))
		for i, j := range jobs {
			if j.ID == "b-07" && !failedOnce {
				failedOnce = true
				errs[i] = errors.New("this one fails once")
				continue
			}
			done[j.ID]++
		}
		return errs
	}
	wctx, stop := context.WithCancel(ctx)
	finished := make(chan struct{})
	go func() {
		_ = q.Work(wctx, nil, jobqueue.WorkOptions{BatchSize: 10, BatchHandlers: map[string]jobqueue.BatchHandler{kind: bh},
			Backoff: func(int) time.Duration { return time.Second }})
		close(finished)
	}()
	deadline := time.Now().Add(60 * time.Second)
	for time.Now().Before(deadline) {
		mu.Lock()
		n := len(done)
		mu.Unlock()
		if n == 25 {
			break
		}
		time.Sleep(50 * time.Millisecond)
	}
	stop()
	<-finished
	mu.Lock()
	defer mu.Unlock()
	if len(done) != 25 {
		t.Fatalf("done %d of 25", len(done))
	}
	for id, n := range done {
		if n > 1 {
			// Delivery is at least once: a broker may hand a job out again (the Pub/Sub emulator does after a nack).
			t.Logf("%s done %d times", id, n)
		}
	}
	if biggest > 10 {
		t.Fatalf("a batch of %d, over the size", biggest)
	}
	t.Logf("largest batch %d", biggest)
}

// Lease checks a Lease: one holder at a time, renewal, release, and expiry.
func Lease(t *testing.T, newLease func(t *testing.T) jobqueue.Lease) {
	ctx := context.Background()
	l := newLease(t)
	name := Kind("lease")
	ok, err := l.Acquire(ctx, name, "a", 2*time.Second)
	if err != nil || !ok {
		t.Fatalf("a takes a free lease: %v %v", ok, err)
	}
	if ok, err = l.Acquire(ctx, name, "b", 2*time.Second); err != nil || ok {
		t.Fatalf("b must not take a held lease: %v %v", ok, err)
	}
	if ok, err = l.Acquire(ctx, name, "a", 2*time.Second); err != nil || !ok {
		t.Fatalf("a renews: %v %v", ok, err)
	}
	if err := l.Release(ctx, name, "b"); err != nil {
		t.Fatalf("release by another holder is a no-op: %v", err)
	}
	if ok, _ = l.Acquire(ctx, name, "b", 2*time.Second); ok {
		t.Fatal("b took it after a release by b")
	}
	if err := l.Release(ctx, name, "a"); err != nil {
		t.Fatal(err)
	}
	if ok, err = l.Acquire(ctx, name, "b", 2*time.Second); err != nil || !ok {
		t.Fatalf("b takes a released lease: %v %v", ok, err)
	}
	// b stops renewing: after its TTL, a takes it.
	time.Sleep(3 * time.Second)
	if ok, err = l.Acquire(ctx, name, "a", 2*time.Second); err != nil || !ok {
		t.Fatalf("a takes an expired lease: %v %v", ok, err)
	}
}

// Store checks a scheduler Store: due order, replacement by id, moving a repeat, and ignoring a replaced entry.
func Store(t *testing.T, newStore func(t *testing.T) jobqueue.Store) {
	ctx := context.Background()
	s := newStore(t)
	kind := Kind("sched")
	base := time.Now().UTC().Truncate(time.Millisecond)
	put := func(id string, due time.Time, every time.Duration) {
		t.Helper()
		if err := s.Put(ctx, jobqueue.Entry{Message: jobqueue.Message{ID: kind + id, Kind: kind, Payload: json.RawMessage(`{"id":"` + id + `"}`)},
			Due: due, Every: every}); err != nil {
			t.Fatal(err)
		}
	}
	put("late", base.Add(time.Hour), 0)
	put("b", base.Add(-time.Minute), 0)
	put("a", base.Add(-2*time.Minute), 0)
	put("rep", base.Add(-30*time.Second), time.Minute)
	mine := func() []jobqueue.Entry {
		all, err := s.Due(ctx, base, 1000)
		if err != nil {
			t.Fatal(err)
		}
		var out []jobqueue.Entry
		for _, e := range all {
			if e.Kind == kind {
				out = append(out, e)
			}
		}
		return out
	}
	due := mine()
	if len(due) != 3 || due[0].ID != kind+"a" || due[1].ID != kind+"b" || due[2].ID != kind+"rep" {
		t.Fatalf("due %+v", due)
	}
	if string(due[0].Payload) != `{"id":"a"}` || due[2].Every != time.Minute {
		t.Fatalf("entry fields lost: %+v", due)
	}
	// Replaced meanwhile: Done with the old due does nothing.
	put("b", base.Add(-10*time.Second), 0)
	if err := s.Done(ctx, due[1], nil); err != nil {
		t.Fatal(err)
	}
	if err := s.Done(ctx, due[0], nil); err != nil {
		t.Fatal(err)
	}
	next := base.Add(30 * time.Second)
	if err := s.Done(ctx, due[2], &next); err != nil {
		t.Fatal(err)
	}
	due = mine()
	if len(due) != 1 || due[0].ID != kind+"b" {
		t.Fatalf("after done: %+v", due)
	}
}

// DoneLog checks a DoneLog: a key is claimed once, and again after Forget.
func DoneLog(t *testing.T, newLog func(t *testing.T) jobqueue.DoneLog) {
	ctx := context.Background()
	l := newLog(t)
	key := Kind("done")
	if ok, err := l.Claim(ctx, key); err != nil || !ok {
		t.Fatalf("first claim: %v %v", ok, err)
	}
	if ok, err := l.Claim(ctx, key); err != nil || ok {
		t.Fatalf("second claim must fail: %v %v", ok, err)
	}
	if err := l.Forget(ctx, key); err != nil {
		t.Fatal(err)
	}
	if ok, err := l.Claim(ctx, key); err != nil || !ok {
		t.Fatalf("claim after forget: %v %v", ok, err)
	}
	ran, err := jobqueue.Once(ctx, l, key+"-once", func(context.Context) error { return errors.New("failed") })
	if !ran || err == nil {
		t.Fatalf("once with a failure: %v %v", ran, err)
	}
	ran, err = jobqueue.Once(ctx, l, key+"-once", func(context.Context) error { return nil })
	if !ran || err != nil {
		t.Fatalf("once after a failure runs again: %v %v", ran, err)
	}
	ran, _ = jobqueue.Once(ctx, l, key+"-once", func(context.Context) error { return nil })
	if ran {
		t.Fatal("once ran twice")
	}
}

// SchedulerOver checks a Queue with a Scheduler over a driver: a delay longer than the driver holds and a repeat
// reach the handler through the scheduler loop.
func SchedulerOver(t *testing.T, d jobqueue.Driver, store jobqueue.Store, lease jobqueue.Lease) {
	ctx := context.Background()
	sched := &jobqueue.Scheduler{Store: store, Lease: lease, Holder: "test", Poll: 200 * time.Millisecond}
	q := jobqueue.New(&noDelay{d}, jobqueue.WithScheduler(sched))
	kind := Kind("scheduled")
	start := time.Now()
	if err := q.Publish(ctx, kind, "later", nil, jobqueue.After(2*time.Second)); err != nil {
		t.Fatal(err)
	}
	if err := q.Publish(ctx, kind, "tick", nil, jobqueue.Every(time.Second)); err != nil {
		t.Fatal(err)
	}
	var mu sync.Mutex
	got := map[string]int{}
	var laterAt time.Duration
	wctx, stop := context.WithCancel(ctx)
	defer stop()
	go func() {
		_ = q.Work(wctx, map[string]jobqueue.Handler{kind: func(_ context.Context, j jobqueue.Job) error {
			mu.Lock()
			defer mu.Unlock()
			id, _, _ := strings.Cut(j.ID, "@") // runs of a repeat carry "@<time>"
			got[id]++
			if j.ID == "later" {
				laterAt = time.Since(start)
			}
			return nil
		}}, jobqueue.WorkOptions{})
	}()
	deadline := time.Now().Add(30 * time.Second)
	for time.Now().Before(deadline) {
		mu.Lock()
		ok := got["later"] == 1 && got["tick"] >= 2
		mu.Unlock()
		if ok {
			if laterAt < 1500*time.Millisecond {
				t.Fatalf("the delayed job came after %s", laterAt)
			}
			return
		}
		time.Sleep(100 * time.Millisecond)
	}
	mu.Lock()
	defer mu.Unlock()
	t.Fatalf("got %v, want later once and tick at least twice", got)
}

// noDelay hides a driver's own delays and repeats, so everything goes through the scheduler.
type noDelay struct{ jobqueue.Driver }

func (noDelay) MaxDelay() time.Duration { return 0 }
