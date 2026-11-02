package redisq_test

import (
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/redis/go-redis/v9"

	"github.com/ez-technologies/jobqueue"
	"github.com/ez-technologies/jobqueue/jobqueuetest"
	"github.com/ez-technologies/jobqueue/memq"
	"github.com/ez-technologies/jobqueue/redisq"
)

func newDriver(t *testing.T) *redisq.Driver {
	t.Helper()
	addr := os.Getenv("JOBQUEUE_TEST_REDIS_ADDR")
	if addr == "" {
		t.Skip("JOBQUEUE_TEST_REDIS_ADDR is not set (eval \"$(scripts/test-env.sh up)\")")
	}
	c := redis.NewClient(&redis.Options{Addr: addr})
	t.Cleanup(func() { _ = c.Close() })
	return redisq.New(c, redisq.Config{Prefix: "jqtest", Poll: 500 * time.Millisecond})
}

func TestDriver(t *testing.T) {
	jobqueuetest.Driver(t, func(t *testing.T) jobqueue.Driver { return newDriver(t) })
}

func TestSchedulerOver(t *testing.T) {
	jobqueuetest.SchedulerOver(t, newDriver(t), memq.NewStore(), memq.NewLease())
}

// A worker that died with a job is replaced: after Visibility another consumer takes the job over.
func TestAStuckJobIsTakenOver(t *testing.T) {
	addr := os.Getenv("JOBQUEUE_TEST_REDIS_ADDR")
	if addr == "" {
		t.Skip("JOBQUEUE_TEST_REDIS_ADDR is not set")
	}
	c := redis.NewClient(&redis.Options{Addr: addr})
	defer c.Close()
	cfg := redisq.Config{Prefix: fmt.Sprintf("jqstuck%d", time.Now().UnixNano()), Poll: 200 * time.Millisecond, Visibility: time.Second}
	cfg.Consumer = "dead-worker"
	dead := redisq.New(c, cfg)
	cfg.Consumer = "live-worker"
	live := redisq.New(c, cfg)
	ctx := t.Context()
	kind := jobqueuetest.Kind("stuck")
	if err := dead.Send(ctx, jobqueue.Message{ID: "s1", Kind: kind}, 0); err != nil {
		t.Fatal(err)
	}
	if got, _ := dead.Receive(ctx, kind); got == nil {
		t.Fatal("not received")
	}
	// dead never acknowledges.
	deadline := time.Now().Add(10 * time.Second)
	for time.Now().Before(deadline) {
		got, err := live.Receive(ctx, kind)
		if err != nil {
			t.Fatal(err)
		}
		if got != nil {
			if got.Message().ID != "s1" || got.Attempt() != 2 {
				t.Fatalf("got %s attempt %d", got.Message().ID, got.Attempt())
			}
			_ = got.Ack(ctx)
			return
		}
	}
	t.Fatal("the stuck job was never taken over")
}
