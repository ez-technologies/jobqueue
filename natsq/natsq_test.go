package natsq_test

import (
	"context"
	"os"
	"testing"

	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"

	"github.com/ez-technologies/jobqueue"
	"github.com/ez-technologies/jobqueue/jobqueuetest"
	"github.com/ez-technologies/jobqueue/memq"
	"github.com/ez-technologies/jobqueue/natsq"
)

// js connects to the test server (JOBQUEUE_TEST_NATS_URL; scripts/test-env.sh starts one).
func js(t *testing.T) jetstream.JetStream {
	t.Helper()
	url := os.Getenv("JOBQUEUE_TEST_NATS_URL")
	if url == "" {
		t.Skip("JOBQUEUE_TEST_NATS_URL is not set (eval \"$(scripts/test-env.sh up)\")")
	}
	nc, err := nats.Connect(url)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(nc.Close)
	j, err := jetstream.New(nc)
	if err != nil {
		t.Fatal(err)
	}
	return j
}

func TestDriver(t *testing.T) {
	jobqueuetest.Driver(t, func(t *testing.T) jobqueue.Driver {
		d, err := natsq.New(context.Background(), js(t), natsq.Config{Prefix: "jqtest", Poll: 500e6})
		if err != nil {
			t.Fatal(err)
		}
		return d
	})
}

func TestRepeatAndSchedulerOver(t *testing.T) {
	d, err := natsq.New(context.Background(), js(t), natsq.Config{Prefix: "jqtest", Poll: 500e6})
	if err != nil {
		t.Fatal(err)
	}
	jobqueuetest.SchedulerOver(t, d, memq.NewStore(), memq.NewLease())
}

func TestLease(t *testing.T) {
	jobqueuetest.Lease(t, func(t *testing.T) jobqueue.Lease {
		l, err := natsq.NewLease(context.Background(), js(t), "jqtest_leases")
		if err != nil {
			t.Fatal(err)
		}
		return l
	})
}

func TestVersions(t *testing.T) {
	jobqueuetest.Versions(t, func(t *testing.T) jobqueue.VersionStore {
		v, err := natsq.NewVersionStore(context.Background(), js(t), "jqtest_versions")
		if err != nil {
			t.Fatal(err)
		}
		return v
	})
}
