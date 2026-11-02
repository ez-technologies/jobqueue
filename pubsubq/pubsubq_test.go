package pubsubq_test

import (
	"context"
	"os"
	"testing"
	"time"

	"github.com/ez-technologies/jobqueue"
	"github.com/ez-technologies/jobqueue/jobqueuetest"
	"github.com/ez-technologies/jobqueue/memq"
	"github.com/ez-technologies/jobqueue/pubsubq"
)

// newDriver talks to the local Pub/Sub emulator (JOBQUEUE_TEST_PUBSUB_EMULATOR); never to Google Cloud.
func newDriver(t *testing.T) *pubsubq.Driver {
	t.Helper()
	host := os.Getenv("JOBQUEUE_TEST_PUBSUB_EMULATOR")
	if host == "" {
		t.Skip("JOBQUEUE_TEST_PUBSUB_EMULATOR is not set (eval \"$(scripts/test-env.sh up)\")")
	}
	t.Setenv("PUBSUB_EMULATOR_HOST", host)
	d, err := pubsubq.New(context.Background(), pubsubq.Config{Project: "jobqueue-test", Prefix: "jqtest",
		Poll: time.Second, CreateResources: true})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = d.Close() })
	return d
}

func TestDriver(t *testing.T) {
	jobqueuetest.Driver(t, func(t *testing.T) jobqueue.Driver { return newDriver(t) })
}

func TestSchedulerOver(t *testing.T) {
	jobqueuetest.SchedulerOver(t, newDriver(t), memq.NewStore(), memq.NewLease())
}

func TestADelayNeedsTheScheduler(t *testing.T) {
	d := newDriver(t)
	if err := d.Send(context.Background(), jobqueue.Message{ID: "x", Kind: jobqueuetest.Kind("d")}, time.Second); err == nil {
		t.Fatal("a delay was accepted")
	}
}
