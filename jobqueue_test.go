package jobqueue_test

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"
	"time"

	"github.com/ez-technologies/jobqueue"
	"github.com/ez-technologies/jobqueue/memq"
)

// limited is a driver that holds no delay of its own.
type limited struct{ jobqueue.Driver }

func (limited) MaxDelay() time.Duration { return time.Second }

func TestPublishRefusesBadKindsAndIDs(t *testing.T) {
	q := jobqueue.New(memq.New())
	ctx := context.Background()
	for _, kind := range []string{"", "a b", "a>b", "*", ".x"} {
		if err := q.Publish(ctx, kind, "id", nil); !errors.Is(err, jobqueue.ErrInvalidKind) {
			t.Errorf("kind %q: %v", kind, err)
		}
	}
	if err := q.Publish(ctx, "ok.kind", "", nil); err == nil {
		t.Error("empty id accepted")
	}
	if err := q.Publish(ctx, "ok", "x", nil, jobqueue.Every(time.Millisecond)); err == nil {
		t.Error("a repeat under a second accepted")
	}
	if err := q.Publish(ctx, "ok", "x", make(chan int)); err == nil {
		t.Error("a payload that is not JSON accepted")
	}
}

func TestADelayTheDriverCannotHoldNeedsAScheduler(t *testing.T) {
	ctx := context.Background()
	d := limited{memq.New()}
	q := jobqueue.New(d)
	if err := q.Publish(ctx, "k", "a", nil, jobqueue.After(500*time.Millisecond)); err != nil {
		t.Fatalf("within the driver's limit: %v", err)
	}
	if err := q.Publish(ctx, "k", "b", nil, jobqueue.After(time.Hour)); !errors.Is(err, jobqueue.ErrNoScheduler) {
		t.Fatalf("beyond it without a scheduler: %v", err)
	}
	if err := q.Publish(ctx, "k", "c", nil, jobqueue.Every(time.Minute)); !errors.Is(err, jobqueue.ErrNoScheduler) {
		t.Fatalf("a repeat without a scheduler: %v", err)
	}
	store := memq.NewStore()
	now := time.Date(2030, 1, 1, 12, 0, 0, 0, time.UTC)
	q = jobqueue.New(d, jobqueue.WithScheduler(&jobqueue.Scheduler{Store: store, Lease: memq.NewLease(), Holder: "me"}),
		jobqueue.WithClock(func() time.Time { return now }))
	if err := q.Publish(ctx, "k", "b", nil, jobqueue.At(now.Add(time.Hour))); err != nil {
		t.Fatal(err)
	}
	if store.Len() != 1 {
		t.Fatalf("store holds %d", store.Len())
	}
}

func TestAPanickingHandlerIsRetried(t *testing.T) {
	ctx := context.Background()
	d := memq.New()
	q := jobqueue.New(d)
	var n atomic.Int32
	done := make(chan struct{})
	_ = q.Publish(ctx, "p", "1", nil)
	wctx, stop := context.WithCancel(ctx)
	defer stop()
	go q.Work(wctx, map[string]jobqueue.Handler{"p": func(context.Context, jobqueue.Job) error {
		if n.Add(1) == 1 {
			panic("boom")
		}
		close(done)
		return nil
	}}, jobqueue.WorkOptions{Backoff: func(int) time.Duration { return 10 * time.Millisecond }})
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("not retried after a panic")
	}
}

func TestWorkNeedsHandlersWithValidKinds(t *testing.T) {
	q := jobqueue.New(memq.New())
	if err := q.Work(context.Background(), nil, jobqueue.WorkOptions{}); err == nil {
		t.Error("no handlers accepted")
	}
	if err := q.Work(context.Background(), map[string]jobqueue.Handler{"a b": nil}, jobqueue.WorkOptions{}); err == nil {
		t.Error("bad kind accepted")
	}
}

func TestDefaultBackoffGrows(t *testing.T) {
	prev := time.Duration(0)
	for i := 0; i <= 7; i++ {
		b := jobqueue.DefaultBackoff(i)
		if b < prev {
			t.Fatalf("attempt %d: %s after %s", i, b, prev)
		}
		prev = b
	}
	if jobqueue.DefaultBackoff(1) != 5*time.Second || jobqueue.DefaultBackoff(99) != 30*time.Minute {
		t.Fatal("unexpected steps")
	}
}

func TestPermanent(t *testing.T) {
	base := errors.New("x")
	if jobqueue.Permanent(nil) != nil {
		t.Fatal("Permanent(nil) must be nil")
	}
	p := jobqueue.Permanent(base)
	if !jobqueue.IsPermanent(p) || !errors.Is(p, base) || jobqueue.IsPermanent(base) {
		t.Fatal("permanent wrapping")
	}
}
