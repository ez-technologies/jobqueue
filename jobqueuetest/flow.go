package jobqueuetest

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/ez-technologies/jobqueue"
	"github.com/ez-technologies/jobqueue/memq"
)

// queue is a Queue over d with an in-memory scheduler, for drivers that hold no (or short) delays.
func queue(d jobqueue.Driver, opts ...jobqueue.Option) *jobqueue.Queue {
	sched := &jobqueue.Scheduler{Store: memq.NewStore(), Lease: memq.NewLease(), Holder: "test", Poll: 100 * time.Millisecond}
	return jobqueue.New(d, append([]jobqueue.Option{jobqueue.WithScheduler(sched)}, opts...)...)
}

// recorder collects what the workers report.
type recorder struct {
	mu       sync.Mutex
	outcomes map[string][]string
	order    []string
	retries  map[string]jobqueue.RetryInfo
}

func newRecorder() *recorder {
	return &recorder{outcomes: map[string][]string{}, retries: map[string]jobqueue.RetryInfo{}}
}

func (r *recorder) onDone(j jobqueue.Job, outcome string, _ error, _ time.Duration) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.outcomes[j.ID] = append(r.outcomes[j.ID], outcome)
	if outcome == "done" {
		r.order = append(r.order, j.ID)
	}
}

func (r *recorder) get(id string) string {
	r.mu.Lock()
	defer r.mu.Unlock()
	return fmt.Sprint(r.outcomes[id])
}

func (r *recorder) waitFor(t *testing.T, within time.Duration, ok func() bool) {
	t.Helper()
	deadline := time.Now().Add(within)
	for time.Now().Before(deadline) {
		if ok() {
			return
		}
		time.Sleep(50 * time.Millisecond)
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	t.Fatalf("not reached; outcomes %v, order %v", r.outcomes, r.order)
}

func work(t *testing.T, q *jobqueue.Queue, handlers map[string]jobqueue.Handler, o jobqueue.WorkOptions) {
	ctx, stop := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() { _ = q.Work(ctx, handlers, o); close(done) }()
	t.Cleanup(func() { stop(); <-done })
}

// Flow checks how the runtime moves jobs on a driver: a failure is re-published (with RetryInfo) and never holds up
// the jobs behind it; a job past its timeout is a failure; lanes; parked keys; skipped retries.
func Flow(t *testing.T, newDriver func(t *testing.T) jobqueue.Driver) {
	ctx := context.Background()
	quick := func(int) time.Duration { return time.Second }

	t.Run("AFailureIsRepublishedAndHoldsNothingUp", func(t *testing.T) {
		q := queue(newDriver(t))
		kind := Kind("hol")
		rec := newRecorder()
		var mu sync.Mutex
		var info *jobqueue.RetryInfo
		failed := atomic.Bool{}
		h := func(ctx context.Context, j jobqueue.Job) error {
			if j.ID == "bad" && failed.CompareAndSwap(false, true) {
				return errors.New("bad input today")
			}
			if j.ID == "bad" {
				mu.Lock()
				info = j.Retry
				mu.Unlock()
			}
			return nil
		}
		_ = q.Publish(ctx, kind, "bad", nil)
		for i := 0; i < 3; i++ {
			_ = q.Publish(ctx, kind, fmt.Sprintf("good-%d", i), nil)
		}
		work(t, q, map[string]jobqueue.Handler{kind: h}, jobqueue.WorkOptions{Backoff: func(int) time.Duration { return 3 * time.Second },
			Jitter: -1, OnDone: rec.onDone})
		rec.waitFor(t, 60*time.Second, func() bool { return rec.get("bad") == "[retry done]" })
		rec.mu.Lock()
		order := append([]string(nil), rec.order...)
		rec.mu.Unlock()
		if order[len(order)-1] != "bad" {
			t.Fatalf("the failed job held others up: %v", order)
		}
		mu.Lock()
		defer mu.Unlock()
		if info == nil || info.Attempt != 2 || info.LastError != "bad input today" || info.FirstFailedAt.IsZero() || info.NextAt.IsZero() {
			t.Fatalf("retry info %+v", info)
		}
	})

	t.Run("AJobPastItsTimeoutFailsAndRunsAgain", func(t *testing.T) {
		q := queue(newDriver(t))
		kind := Kind("timeout")
		rec := newRecorder()
		var calls atomic.Int32
		var lastErr atomic.Value
		h := func(ctx context.Context, j jobqueue.Job) error {
			if calls.Add(1) == 1 {
				time.Sleep(1500 * time.Millisecond) // ignores its ctx: still counted as failed at the timeout
				return nil
			}
			if j.Retry != nil {
				lastErr.Store(j.Retry.LastError)
			}
			return nil
		}
		_ = q.Publish(ctx, kind, "slow", nil)
		work(t, q, map[string]jobqueue.Handler{kind: h}, jobqueue.WorkOptions{Timeout: 300 * time.Millisecond, Backoff: quick, Jitter: -1,
			OnDone: rec.onDone})
		rec.waitFor(t, 60*time.Second, func() bool { return rec.get("slow") == "[retry done]" })
		if e, _ := lastErr.Load().(string); !strings.Contains(e, "timed out") {
			t.Fatalf("last error %q", e)
		}
	})

	t.Run("ALaneHasItsOwnWorkers", func(t *testing.T) {
		d := newDriver(t)
		q := queue(d)
		kind := Kind("lane")
		rec := newRecorder()
		h := func(context.Context, jobqueue.Job) error { return nil }
		lanes := map[string]jobqueue.Lane{"slow": {Concurrency: 1, Timeout: time.Minute}}
		_ = q.Publish(ctx, kind, "fast-1", nil)
		_ = q.Publish(ctx, kind, "heavy-1", nil, jobqueue.InLane("slow"))
		work(t, q, map[string]jobqueue.Handler{kind: h}, jobqueue.WorkOptions{Lanes: lanes, OnlyLanes: []string{"default"}, OnDone: rec.onDone})
		rec.waitFor(t, 30*time.Second, func() bool { return rec.get("fast-1") == "[done]" })
		time.Sleep(time.Second)
		if rec.get("heavy-1") != "[]" {
			t.Fatal("the default lane's worker took a job of the slow lane")
		}
		work(t, queue(d), map[string]jobqueue.Handler{kind: h}, jobqueue.WorkOptions{Lanes: lanes, OnlyLanes: []string{"slow"}, OnDone: rec.onDone})
		rec.waitFor(t, 30*time.Second, func() bool { return rec.get("heavy-1") == "[done]" })
	})

	t.Run("AKeyThatKeepsFailingIsParkedAndOnlyItWaits", func(t *testing.T) {
		q := queue(newDriver(t))
		kind := Kind("park")
		rec := newRecorder()
		var parked atomic.Int32
		h := func(ctx context.Context, j jobqueue.Job) error {
			if j.ID == "a-1" {
				return errors.New("always fails")
			}
			return nil
		}
		for _, p := range []struct{ id, key string }{{"a-1", "a"}, {"a-2", "a"}, {"b-1", "b"}, {"a-3", "a"}, {"b-2", "b"}} {
			_ = q.Publish(ctx, kind, p.id, nil, jobqueue.Key(p.key))
		}
		work(t, q, map[string]jobqueue.Handler{kind: h}, jobqueue.WorkOptions{Concurrency: 2, KeyAttempts: 2, Backoff: func(int) time.Duration { return 100 * time.Millisecond },
			OnDone: rec.onDone, OnParked: func(jobqueue.Job, error) { parked.Add(1) }})
		rec.waitFor(t, 60*time.Second, func() bool {
			return rec.get("a-3") == "[parked]" && rec.get("b-2") == "[done]" && rec.get("a-2") == "[parked]"
		})
		if got := rec.get("a-1"); got != "[retry parked]" {
			t.Fatalf("a-1: %s", got)
		}
		if rec.get("b-1") != "[done]" || parked.Load() != 1 {
			t.Fatalf("b-1 %s, parked alerts %d", rec.get("b-1"), parked.Load())
		}
	})

	t.Run("ARetryOfASupersededVersionIsSkipped", func(t *testing.T) {
		versions := jobqueue.NewMemoryVersions()
		q := queue(newDriver(t), jobqueue.WithVersions(versions))
		kind := Kind("sync")
		rec := newRecorder()
		release := make(chan struct{})
		var once sync.Map
		h := func(ctx context.Context, j jobqueue.Job) error {
			if _, again := once.LoadOrStore(j.ID, true); !again && (j.ID == "v1" || j.ID == "v3" || j.ID == "v4") {
				return errors.New("fails the first time")
			}
			if j.ID == "v5" {
				<-release // the newer job is still running when v4's retry comes
			}
			return nil
		}
		work(t, q, map[string]jobqueue.Handler{kind: h}, jobqueue.WorkOptions{Concurrency: 2, Mode: jobqueue.Unordered,
			Backoff: func(int) time.Duration { return 2 * time.Second }, Jitter: -1, ShouldSkip: jobqueue.Supersede(versions), OnDone: rec.onDone})
		// Superseded: v1 fails, v2 is published before its retry.
		_ = q.Publish(ctx, kind, "v1", nil, jobqueue.Version("topic-7", 1))
		_ = q.Publish(ctx, kind, "v2", nil, jobqueue.Version("topic-7", 2))
		// Not superseded: v3 of another entity fails and nothing newer comes.
		_ = q.Publish(ctx, kind, "v3", nil, jobqueue.Version("topic-8", 3))
		// A race: v4 fails; v5, newer, is published and still running when v4's retry comes.
		_ = q.Publish(ctx, kind, "v4", nil, jobqueue.Version("topic-9", 4))
		_ = q.Publish(ctx, kind, "v5", nil, jobqueue.Version("topic-9", 5))
		rec.waitFor(t, 60*time.Second, func() bool {
			return rec.get("v1") == "[retry skipped]" && rec.get("v3") == "[retry done]" && rec.get("v4") == "[retry skipped]"
		})
		close(release)
		rec.waitFor(t, 30*time.Second, func() bool { return rec.get("v5") == "[done]" && rec.get("v2") == "[done]" })
	})

	t.Run("ARetryHandlerRunsTheCopy", func(t *testing.T) {
		q := queue(newDriver(t))
		kind := Kind("rh")
		rec := newRecorder()
		var got atomic.Value
		h := func(context.Context, jobqueue.Job) error { return errors.New("first path fails") }
		rh := func(_ context.Context, j jobqueue.Job, info jobqueue.RetryInfo) error {
			got.Store(info.LastError)
			return nil
		}
		_ = q.Publish(ctx, kind, "r", nil)
		work(t, q, map[string]jobqueue.Handler{kind: h}, jobqueue.WorkOptions{Backoff: quick, Jitter: -1, OnDone: rec.onDone,
			RetryHandlers: map[string]jobqueue.RetryHandler{kind: rh}})
		rec.waitFor(t, 60*time.Second, func() bool { return rec.get("r") == "[retry done]" })
		if got.Load() != "first path fails" {
			t.Fatalf("retry handler saw %v", got.Load())
		}
	})
}

// Versions checks a VersionStore: it keeps the highest version, also under concurrent observers.
func Versions(t *testing.T, newStore func(t *testing.T) jobqueue.VersionStore) {
	ctx := context.Background()
	s := newStore(t)
	key := Kind("versions")
	if _, ok, err := s.Latest(ctx, key); err != nil || ok {
		t.Fatalf("an unseen key: %v %v", ok, err)
	}
	for _, v := range []int64{3, 7, 5} {
		if err := s.Observe(ctx, key, v); err != nil {
			t.Fatal(err)
		}
	}
	if v, ok, err := s.Latest(ctx, key); err != nil || !ok || v != 7 {
		t.Fatalf("latest %d %v %v, want 7", v, ok, err)
	}
	var wg sync.WaitGroup
	for i := int64(1); i <= 20; i++ {
		wg.Go(func() { _ = s.Observe(ctx, key, 100+i) })
	}
	wg.Wait()
	if v, _, _ := s.Latest(ctx, key); v != 120 {
		t.Fatalf("after concurrent observes: %d, want 120", v)
	}
}

// Parking checks a ParkStore.
func Parking(t *testing.T, newStore func(t *testing.T) jobqueue.ParkStore) {
	ctx := context.Background()
	s := newStore(t)
	kind := Kind("parked")
	if p, err := s.Parked(ctx, kind, "a"); err != nil || p {
		t.Fatalf("parked before: %v %v", p, err)
	}
	if err := s.Park(ctx, kind, "a", "it kept failing"); err != nil {
		t.Fatal(err)
	}
	if err := s.Park(ctx, kind, "a", "again"); err != nil {
		t.Fatal(err)
	}
	if p, _ := s.Parked(ctx, kind, "a"); !p {
		t.Fatal("not parked")
	}
	if p, _ := s.Parked(ctx, kind, "b"); p {
		t.Fatal("another key parked")
	}
	if err := s.Unpark(ctx, kind, "a"); err != nil {
		t.Fatal(err)
	}
	if p, _ := s.Parked(ctx, kind, "a"); p {
		t.Fatal("still parked")
	}
}
